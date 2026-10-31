import asyncio
import logging
import re
import uuid
from difflib import SequenceMatcher

import httpx

from fastapi import APIRouter, HTTPException, Query


router = APIRouter(prefix="/api/v1", tags=["Chapters"])

MANGADEX_URL = "https://api.mangadex.org"
ANILIST_URL = "https://graphql.anilist.co"

HEADERS = {
    "User-Agent": "MangaAITranslator/5.0",
    "Accept": "application/json",
}

LANGUAGES = {
    "ar": "العربية",
    "en": "الإنجليزية",
    "fr": "الفرنسية",
    "ko": "الكورية",
    "ja": "اليابانية",
    "zh": "الصينية",
    "es": "الإسبانية",
    "pt-br": "البرتغالية",
    "ru": "الروسية",
    "de": "الألمانية",
    "it": "الإيطالية",
    "tr": "التركية",
    "id": "الإندونيسية",
    "th": "التايلاندية",
    "vi": "الفيتنامية",
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

semaphore = asyncio.Semaphore(3)


# ==================================================
# TEXT HELPERS
# ==================================================

def normalize_title(value):
    if not value:
        return ""

    value = str(value).lower().strip()
    value = re.sub(r"[^\w\s]", " ", value)
    value = re.sub(r"\s+", " ", value)

    return value.strip()


def title_similarity(first, second):
    first = normalize_title(first)
    second = normalize_title(second)

    if not first or not second:
        return 0

    if first == second:
        return 1.0

    return SequenceMatcher(None, first, second).ratio()


def extract_titles(media):
    titles = media.get("title") or {}

    result = []

    for key in ("english", "romaji", "native"):
        value = titles.get(key)

        if value:
            result.append(value)

    for synonym in media.get("synonyms") or []:
        if synonym:
            result.append(synonym)

    return list(dict.fromkeys(result))


# ==================================================
# ANILIST
# ==================================================

async def fetch_anilist(manga_id):
    query = """
    query ($id: Int) {
        Media(id: $id, type: MANGA) {
            id
            idMal
            title {
                romaji
                english
                native
            }
            synonyms
            chapters
            volumes
            status
            coverImage {
                large
            }
            siteUrl
        }
    }
    """

    try:
        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=25,
        ) as client:

            response = await client.post(
                ANILIST_URL,
                json={
                    "query": query,
                    "variables": {
                        "id": int(manga_id)
                    },
                },
            )

            response.raise_for_status()

            data = response.json()

            if data.get("errors"):
                logger.error("AniList errors: %s", data["errors"])
                return None

            return data.get("data", {}).get("Media")

    except (ValueError, httpx.HTTPError):
        logger.exception("AniList request failed")
        return None


# ==================================================
# MANGADEX SEARCH
# ==================================================

async def search_mangadex(title):
    if not title:
        return []

    params = {
        "title": title,
        "limit": 10,
        "includes[]": ["cover_art"],
    }

    try:
        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=25,
        ) as client:

            response = await client.get(
                f"{MANGADEX_URL}/manga",
                params=params,
            )

            response.raise_for_status()

            return response.json().get("data", [])

    except httpx.HTTPError:
        logger.exception("MangaDex search failed")
        return []


def get_mangadex_titles(manga):
    attributes = manga.get("attributes", {})

    titles = list(
        (attributes.get("title") or {}).values()
    )

    for alternative in attributes.get("altTitles", []):
        titles.extend(alternative.values())

    return [title for title in titles if title]


def match_mangadex(anilist_media, candidates):
    anilist_titles = extract_titles(anilist_media)

    best_match = None
    best_score = 0

    for candidate in candidates:
        candidate_titles = get_mangadex_titles(candidate)

        score = 0

        for first in anilist_titles:
            for second in candidate_titles:
                score = max(
                    score,
                    title_similarity(first, second),
                )

        if score > best_score:
            best_score = score
            best_match = candidate

    if best_score < 0.72:
        return None, best_score

    return best_match, best_score


# ==================================================
# RESOLVE MANGA ID
# ==================================================

async def resolve_manga(identifier):
    """
    Accepts either an AniList numeric ID
    or a MangaDex UUID.
    """

    try:
        uuid.UUID(identifier)

        return {
            "source_id": identifier,
            "anilist": None,
            "mangadex": identifier,
            "match_score": 1.0,
        }

    except (ValueError, AttributeError):
        pass

    if not str(identifier).isdigit():
        raise HTTPException(
            status_code=400,
            detail="Invalid manga ID",
        )

    anilist = await fetch_anilist(identifier)

    if not anilist:
        raise HTTPException(
            status_code=404,
            detail="Manga not found in AniList",
        )

    titles = extract_titles(anilist)

    candidates = []

    for title in titles:
        results = await search_mangadex(title)
        candidates.extend(results)

        if candidates:
            break

    if not candidates:
        raise HTTPException(
            status_code=404,
            detail="Manga not found in MangaDex",
        )

    mangadex, score = match_mangadex(
        anilist,
        candidates,
    )

    if not mangadex:
        raise HTTPException(
            status_code=404,
            detail="No reliable MangaDex title match found",
        )

    return {
        "source_id": identifier,
        "anilist": anilist,
        "mangadex": mangadex.get("id"),
        "match_score": round(score, 3),
    }


# ==================================================
# CHAPTER FETCHING
# ==================================================

async def fetch_language(client, manga_id, code):
    async with semaphore:

        params = {
            "translatedLanguage[]": code,
            "limit": 1,
            "order[readableAt]": "desc",
        }

        try:
            response = await client.get(
                f"{MANGADEX_URL}/manga/{manga_id}/feed",
                params=params,
                timeout=25,
            )

            response.raise_for_status()

            data = response.json()
            chapters = data.get("data", [])

            latest = None

            if chapters:
                chapter = chapters[0]
                attr = chapter.get("attributes", {})
                chapter_id = chapter.get("id")

                latest = {
                    "id": chapter_id,
                    "chapter": attr.get("chapter"),
                    "title": attr.get("title"),
                    "volume": attr.get("volume"),
                    "published_at": attr.get("readableAt"),
                    "url": (
                        f"https://mangadex.org/chapter/"
                        f"{chapter_id}"
                    ),
                }

            return {
                "language_code": code,
                "language_name": LANGUAGES[code],
                "chapter_count": data.get("total", 0),
                "latest_chapter": latest,
            }

        except httpx.HTTPError as error:
            logger.exception(
                "Chapter language request failed: %s",
                code,
            )

            return {
                "language_code": code,
                "language_name": LANGUAGES[code],
                "chapter_count": None,
                "latest_chapter": None,
                "error": str(error),
            }


# ==================================================
# OLD CHAPTER SUMMARY ROUTE
# ==================================================

@router.get("/manga/{manga_id}/chapters")
async def get_manga_chapters(
    manga_id: str,
    languages: list[str] | None = Query(default=None),
):
    selected = languages if languages else list(LANGUAGES)

    if len(selected) > 15:
        raise HTTPException(
            status_code=400,
            detail="Maximum 15 languages",
        )

    unknown = [
        code for code in selected
        if code not in LANGUAGES
    ]

    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported languages: {unknown}",
        )

    resolved = await resolve_manga(manga_id)
    mangadex_id = resolved["mangadex"]

    async with httpx.AsyncClient(
        headers=HEADERS,
    ) as client:

        results = await asyncio.gather(
            *[
                fetch_language(
                    client,
                    mangadex_id,
                    code,
                )
                for code in selected
            ]
        )

    arabic = next(
        (
            item for item in results
            if item["language_code"] == "ar"
        ),
        None,
    )

    if arabic is None:
        arabic_status = "not_checked"

    elif arabic["chapter_count"] is None:
        arabic_status = "unknown"

    elif arabic["chapter_count"] > 0:
        arabic_status = "available"

    else:
        arabic_status = "not_available"

    available = sum(
        1 for item in results
        if item["chapter_count"] is not None
        and item["chapter_count"] > 0
    )

    return {
        "source_id": manga_id,
        "anilist_id": (
            resolved["anilist"]["id"]
            if resolved["anilist"]
            else None
        ),
        "mangadex_id": mangadex_id,
        "match_score": resolved["match_score"],
        "source": "AniList + MangaDex",
        "arabic_status": arabic_status,
        "available_languages": available,
        "anilist_chapters": (
            resolved["anilist"].get("chapters")
            if resolved["anilist"]
            else None
        ),
        "languages": results,
    }


# ==================================================
# FULL CHAPTER LIST
# ==================================================

@router.get("/manga/{manga_id}/chapter-list")
async def get_chapter_list(
    manga_id: str,
    language: str = Query(default="ar"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):

    if language not in LANGUAGES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported language: {language}",
        )

    resolved = await resolve_manga(manga_id)
    mangadex_id = resolved["mangadex"]

    params = {
        "translatedLanguage[]": language,
        "limit": limit,
        "offset": offset,
        "order[chapter]": "asc",
        "includes[]": ["scanlation_group"],
    }

    try:
        async with httpx.AsyncClient(
            headers=HEADERS,
            timeout=30,
        ) as client:

            response = await client.get(
                f"{MANGADEX_URL}/manga/{mangadex_id}/feed",
                params=params,
            )

            response.raise_for_status()

            data = response.json()

        chapters = []

        for item in data.get("data", []):
                        attr = item.get("attributes", {})
            chapter_id = item.get("id")

            group = None
            for relation in item.get("relationships", []):
                if relation.get("type") == "scanlation_group":
                    group = relation.get("attributes", {}).get("name")
                    break

            chapters.append({
                "id": chapter_id,
                "chapter": attr.get("chapter"),
                "title": attr.get("title"),
                "volume": attr.get("volume"),
                "language": attr.get("translatedLanguage"),
                "pages": attr.get("pages"),
                "published_at": attr.get("readableAt"),
                "group": group,
                "url": f"https://mangadex.org/chapter/{chapter_id}",
            })

        return {
            "manga_id": manga_id,
            "mangadex_id": mangadex_id,
            "language": language,
            "total": data.get("total", 0),
            "limit": limit,
            "offset": offset,
            "chapters": chapters,
        }

    except httpx.TimeoutException:
        raise HTTPException(
            status_code=504,
            detail="MangaDex request timed out",
        )

    except httpx.HTTPStatusError as error:
        raise HTTPException(
            status_code=502,
            detail=f"MangaDex error: {error.response.status_code}",
        )

    except httpx.RequestError:
        raise HTTPException(
            status_code=502,
            detail="Unable to connect to MangaDex",
        )
