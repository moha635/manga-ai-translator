import asyncio
import logging
import re
import unicodedata
from typing import Any

import httpx
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from chapters import router as chapters_router


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("manga-ai")

APP_VERSION = "4.2.1"

app = FastAPI(
    title="Manga AI Translator",
    version=APP_VERSION
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chapters_router)


ANILIST_URL = "https://graphql.anilist.co"
MANGADEX_URL = "https://api.mangadex.org"
MANGABAKA_URL = "https://api.mangabaka.org/v2"


ANILIST_QUERY = """
query ($search: String!, $perPage: Int!) {
  Page(perPage: $perPage) {
    media(search: $search, type: MANGA, sort: SEARCH_MATCH) {
      id
      title {
        romaji
        english
        native
      }
      description(asHtml: false)
      coverImage {
        large
        extraLarge
      }
      averageScore
      popularity
      status
      chapters
      volumes
      genres
      countryOfOrigin
      startDate {
        year
        month
        day
      }
      siteUrl
      format
      isAdult
    }
  }
}
"""


# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def safe_text(value: Any) -> str:
    if value is None:
        return ""

    if isinstance(value, (dict, list)):
        return ""

    return str(value).strip()


def safe_number(value: Any):
    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        return value

    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None

    return None


def popularity_number(value: Any) -> float:
    """
    MangaBaka popularity may be a dictionary.
    Extract a usable numeric value safely.
    """

    direct = safe_number(value)

    if direct is not None:
        return direct

    if isinstance(value, dict):
        preferred_keys = [
            "global",
            "all",
            "rank",
            "total",
            "value"
        ]

        for key in preferred_keys:
            number = safe_number(value.get(key))

            if number is not None:
                return number

    return 0


def normalize_name(value: Any) -> str:
    text = safe_text(value).casefold()

    text = unicodedata.normalize("NFKC", text)

    text = re.sub(r"[^\w\s]", " ", text)

    return " ".join(text.split())


def first_title(titles, language="en"):

    if isinstance(titles, str):
        return titles

    if not isinstance(titles, list):
        return ""

    matching = [
        item for item in titles
        if isinstance(item, dict)
        and item.get("language") == language
    ]

    for item in matching:
        if item.get("is_primary"):
            return safe_text(item.get("title"))

    for item in matching:
        traits = item.get("traits") or []

        if isinstance(traits, list) and "official" in traits:
            return safe_text(item.get("title"))

    if matching:
        return safe_text(matching[0].get("title"))

    return ""


def mangabaka_cover(value):

    if isinstance(value, str):
        return value

    if isinstance(value, dict):
        return (
            value.get("x350")
            or value.get("x250")
            or value.get("x150")
            or value.get("raw")
        )

    return None


def is_novel_format(value: Any) -> bool:
    manga_format = safe_text(value).casefold()

    return (
        "novel" in manga_format
        or "light novel" in manga_format
    )


def normalize_genres(value: Any) -> list:

    if not isinstance(value, list):
        return []

    genres = []

    for item in value:

        if isinstance(item, str):
            name = item.strip()

        elif isinstance(item, dict):
            name = (
                safe_text(item.get("name"))
                or safe_text(item.get("en"))
                or safe_text(item.get("title"))
            )

        else:
            continue

        if name:
            genres.append(name)

    return list(dict.fromkeys(genres))


# --------------------------------------------------
# ANILIST
# --------------------------------------------------

async def search_anilist(client, query, limit):

    response = await client.post(
        ANILIST_URL,
        json={
            "query": ANILIST_QUERY,
            "variables": {
                "search": query,
                "perPage": limit
            }
        }
    )

    response.raise_for_status()

    payload = response.json()

    if payload.get("errors"):
        raise ValueError("AniList returned GraphQL errors")

    media = (
        payload.get("data", {})
        .get("Page", {})
        .get("media", [])
    )

    results = []

    for item in media:

        if item.get("isAdult"):
            continue

        manga_format = safe_text(
            item.get("format")
        ).upper()

        if is_novel_format(manga_format):
            continue

        title = item.get("title") or {}
        cover = item.get("coverImage") or {}

        results.append({
            "id": f"anilist-{item['id']}",
            "source": "AniList",

            "title": {
                "english": safe_text(title.get("english")),
                "romaji": safe_text(title.get("romaji")),
                "native": safe_text(title.get("native"))
            },

            "description": safe_text(
                item.get("description")
            ),

            "cover": (
                cover.get("extraLarge")
                or cover.get("large")
            ),

            "rating": safe_number(
                item.get("averageScore")
            ),

            "popularity": safe_number(
                item.get("popularity")
            ),

            "status": item.get("status"),

            "chapters": item.get("chapters"),

            "genres": normalize_genres(
                item.get("genres")
            ),

            "country": item.get("countryOfOrigin"),

            "release_date": item.get("startDate"),

            "format": manga_format,

            "url": item.get("siteUrl")
        })

    return results


# --------------------------------------------------
# MANGADEX
# --------------------------------------------------

async def search_mangadex(client, query, limit):

    params = [
        ("title", query),
        ("limit", limit),
        ("includes[]", "cover_art"),
        ("contentRating[]", "safe"),
        ("contentRating[]", "suggestive"),
        ("order[relevance]", "desc")
    ]

    response = await client.get(
        f"{MANGADEX_URL}/manga",
        params=params
    )

    response.raise_for_status()

    payload = response.json()

    if payload.get("result") != "ok":
        raise ValueError("MangaDex returned an error")

    results = []

    for item in payload.get("data", []):

        attributes = item.get("attributes") or {}

        titles = attributes.get("title") or {}

        cover_file = None

        for relation in item.get("relationships", []):

            if relation.get("type") == "cover_art":

                cover_file = (
                    relation.get("attributes", {})
                    .get("fileName")
                )

                break

        cover_url = None

        if cover_file:

            cover_url = (
                "https://uploads.mangadex.org/covers/"
                f"{item['id']}/{cover_file}.512.jpg"
            )

        english_title = (
            titles.get("en")
            or titles.get("en-us")
            or ""
        )

        native_title = (
            titles.get("ko")
            or titles.get("ja")
            or titles.get("zh")
            or ""
        )

        results.append({

            "id": f"mangadex-{item['id']}",

            "source": "MangaDex",

            "title": {
                "english": safe_text(english_title),

                "romaji": safe_text(
                    next(iter(titles.values()), "")
                ),

                "native": safe_text(native_title)
            },

            "description": safe_text(
                (attributes.get("description") or {}).get("en")
            ),

            "cover": cover_url,

            "rating": None,

            "popularity": None,

            "status": attributes.get("status"),

            "chapters": None,

            "genres": normalize_genres([
                tag.get("attributes", {})
                .get("name", {})
                .get("en")

                for tag in attributes.get("tags", [])
            ]),

            "country": attributes.get("originalLanguage"),

            "release_date": attributes.get("year"),

            "format": None,

            "url": f"https://mangadex.org/title/{item['id']}"
        })

    return results


# --------------------------------------------------
# MANGABAKA
# --------------------------------------------------

async def search_mangabaka(client, query, limit):

    params = [
        ("q", query),
        ("limit", limit),
        ("content_rating", "safe"),
        ("content_rating", "suggestive")
    ]

    response = await client.get(
        f"{MANGABAKA_URL}/series/search",
        params=params
    )

    response.raise_for_status()

    payload = response.json()

    if payload.get("status", response.status_code) != 200:
        raise ValueError("MangaBaka returned an error")

    results = []

    for item in payload.get("data", []):

        if not isinstance(item, dict):
            continue

        manga_format = safe_text(
            item.get("format")
            or item.get("type")
        )

        # Exclude novels
        if is_novel_format(manga_format):
            continue

        titles = item.get("titles") or []

        english = (
            first_title(titles, "en")
            or safe_text(item.get("title"))
        )

        japanese = first_title(titles, "ja")
        korean = first_title(titles, "ko")
        chinese = first_title(titles, "zh")

        native = (
            japanese
            or korean
            or chinese
        )

        series_id = item.get("id")

        if series_id is None:
            continue

        chapters = (
            item.get("chapters")
            if item.get("chapters") is not None
            else item.get("total_chapters")
        )

        release_date = (
            item.get("release_date")
            or item.get("published")
        )

        results.append({

            "id": f"mangabaka-{series_id}",

            "source": "MangaBaka",

            "title": {
                "english": english,
                "romaji": english,
                "native": native
            },

            "description": safe_text(
                item.get("description")
            ),

            "cover": mangabaka_cover(
                item.get("cover")
            ),

            "rating": safe_number(
                item.get("rating")
            ),

            "popularity": popularity_number(
                item.get("popularity")
            ),

            "status": item.get("status"),

            "chapters": chapters,

            "genres": normalize_genres(
                item.get("genres")
            ),

            "country": None,

            "release_date": release_date,

            "format": manga_format,

            "url": (
                f"https://mangabaka.org/series/{series_id}"
            )
        })

    return results


# --------------------------------------------------
# MERGE RESULTS
# --------------------------------------------------

def merge_results(results):

    unique = []

    for item in results:

        titles = item.get("title") or {}

        names = {
            normalize_name(value)

            for value in titles.values()

            if normalize_name(value)
        }

        matched = None

        for existing in unique:

            # Never merge two results from the same source.
            if existing.get("source") == item.get("source"):
                continue

            existing_titles = existing.get("title") or {}

            existing_names = {
                normalize_name(value)

                for value in existing_titles.values()

                if normalize_name(value)
            }

            # Only exact normalized title matches.
            if names.intersection(existing_names):
                matched = existing
                break

        if matched is None:

            new_item = dict(item)

            new_item["title"] = dict(
                item.get("title") or {}
            )

            new_item["genres"] = list(
                item.get("genres") or []
            )

            new_item["other_sources"] = []

            unique.append(new_item)

            continue

        source = item.get("source")

        if (
            source
            and source != matched.get("source")
            and source not in matched["other_sources"]
        ):
            matched["other_sources"].append(source)

        fields = [
            "description",
            "cover",
            "rating",
            "popularity",
            "status",
            "chapters",
            "country",
            "release_date",
            "format",
            "url"
        ]

        for field in fields:

            if (
                matched.get(field) is None
                or matched.get(field) == ""
            ):

                if item.get(field) is not None:
                    matched[field] = item[field]

        for language in (
            "english",
            "romaji",
            "native"
        ):

            if not matched["title"].get(language):

                matched["title"][language] = (
                    (item.get("title") or {}).get(language)
                )

        matched["genres"] = list(dict.fromkeys(
            (matched.get("genres") or [])
            + (item.get("genres") or [])
        ))

    return unique


# --------------------------------------------------
# RELEVANCE AND SORTING
# --------------------------------------------------

def calculate_relevance(item, query):

    normalized_query = normalize_name(query)

    titles = item.get("title") or {}

    names = [
        normalize_name(value)

        for value in titles.values()

        if normalize_name(value)
    ]

    if not names:
        return 0

    if normalized_query in names:
        return 100

    if any(
        name.startswith(normalized_query)
        for name in names
    ):
        return 80

    if any(
        normalized_query in name
        for name in names
    ):
        return 60

    return 10


def sort_results(results, query):

    return sorted(
        results,
        key=lambda item: (
            calculate_relevance(item, query),
            safe_number(item.get("rating")) or 0,
            popularity_number(item.get("popularity"))
        ),
        reverse=True
    )


# --------------------------------------------------
# API ROUTES
# --------------------------------------------------

@app.get("/")
async def home():

    return {
        "name": "Manga AI Translator",

        "version": APP_VERSION,

        "status": "running",

        "sources": [
            "AniList",
            "MangaDex",
            "MangaBaka"
        ],

        "features": [
            "catalog_search",
            "chapter_language_detection",
            "multi_source_search",
            "result_merging",
            "novel_filtering",
            "search_relevance"
        ]
    }


@app.get("/health")
async def health():

    return {
        "status": "healthy",
        "version": APP_VERSION
    }


# --------------------------------------------------
# CATALOG SEARCH
# --------------------------------------------------

@app.get("/api/v1/catalog/search")
async def catalog_search(

    q: str = Query(
        ...,
        min_length=1,
        max_length=100
    ),

    limit: int = Query(
        10,
        ge=1,
        le=20
    )

):

    headers = {
        "User-Agent": "MangaAITranslator/4.2.1",
        "Accept": "application/json"
    }

    timeout = httpx.Timeout(25.0)

    async with httpx.AsyncClient(
        headers=headers,
        timeout=timeout,
        follow_redirects=True
    ) as client:

        source_names = [
            "AniList",
            "MangaDex",
            "MangaBaka"
        ]

        responses = await asyncio.gather(

            search_anilist(client, q, limit),

            search_mangadex(client, q, limit),

            search_mangabaka(client, q, limit),

            return_exceptions=True
        )

    results = []

    errors = []

    successful_sources = []

    for name, response in zip(
        source_names,
        responses
    ):

        if isinstance(response, Exception):

            logger.warning(
                "%s failed: %s",
                name,
                response
            )

            errors.append(name)

        else:

            successful_sources.append(name)

            results.extend(response)

    final_results = merge_results(results)

    final_results = sort_results(
        final_results,
        q
    )

    return {

        "query": q,

        "total": len(final_results),

        "sources_checked": source_names,

        "sources_successful": successful_sources,

        "sources_with_errors": errors,

        "results": final_results
    }
