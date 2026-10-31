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

APP_VERSION = "4.4.0"

app = FastAPI(title="Manga AI Translator", version=APP_VERSION)

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


# ==================================================
# ANILIST SEARCH
# ==================================================

ANILIST_QUERY = """
query ($search: String!, $perPage: Int!) {
  Page(perPage: $perPage) {
    media(search: $search, type: MANGA, sort: SEARCH_MATCH) {
      id
      title { romaji english native }
      description(asHtml: false)
      coverImage { large extraLarge }
      averageScore
      popularity
      status
      chapters
      volumes
      genres
      countryOfOrigin
      startDate { year month day }
      siteUrl
      format
      isAdult
    }
  }
}
"""


# ==================================================
# DISCOVERY QUERY
# ==================================================

DISCOVER_QUERY = """
query (
  $page: Int!,
  $perPage: Int!,
  $sort: [MediaSort],
  $status: MediaStatus
) {
  Page(page: $page, perPage: $perPage) {
    pageInfo {
      total
      currentPage
      lastPage
      hasNextPage
    }
    media(
      type: MANGA,
      sort: $sort,
      status: $status,
      isAdult: false
    ) {
      id
      title { romaji english native }
      description(asHtml: false)
      coverImage { large extraLarge }
      averageScore
      popularity
      trending
      status
      chapters
      volumes
      genres
      countryOfOrigin
      startDate { year month day }
      siteUrl
      format
      isAdult
    }
  }
}
"""


# ==================================================
# HELPERS
# ==================================================

def safe_text(value: Any) -> str:
    if value is None or isinstance(value, (dict, list)):
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
    direct = safe_number(value)

    if direct is not None:
        return direct

    if isinstance(value, dict):
        for key in ("score", "value", "popularity"):
            number = safe_number(value.get(key))
            if number is not None:
                return number

    return 0.0


def normalize_name(value: Any) -> str:
    text = safe_text(value).casefold()
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[^\w\s]", " ", text)
    return " ".join(text.split())


def title_values(item: dict) -> list[str]:
    titles = item.get("title") or {}

    if isinstance(titles, str):
        values = [titles]
    elif isinstance(titles, dict):
        values = list(titles.values())
    else:
        values = []

    return list(dict.fromkeys(
        safe_text(value)
        for value in values
        if safe_text(value)
    ))


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
    return "novel" in safe_text(value).casefold()


def normalize_genres(value: Any) -> list[str]:
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


def is_doujinshi(item: dict) -> bool:
    title_text = " ".join(title_values(item)).casefold()
    format_text = safe_text(item.get("format")).casefold()
    genres = " ".join(item.get("genres") or []).casefold()

    return (
        "doujinshi" in title_text
        or "doujinshi" in format_text
        or "doujinshi" in genres
    )


def title_match_score(item: dict, query: str) -> int:
    q = normalize_name(query)

    if not q:
        return 0

    q_tokens = q.split()
    best = 0

    for raw_title in title_values(item):
        name = normalize_name(raw_title)

        if not name:
            continue

        if name == q:
            score = 100

        elif name.startswith(q):
            score = 90

        elif q in name:
            score = 85

        else:
            title_tokens = set(name.split())
            matched = sum(
                token in title_tokens
                for token in q_tokens
            )

            if matched == len(q_tokens):
                score = 75

            elif len(q_tokens) == 1 and matched == 1:
                score = 65

            else:
                score = 0

        best = max(best, score)

    return best


def filter_search_results(
    results: list[dict],
    query: str
) -> list[dict]:

    filtered = []
    seen_ids = set()

    for item in results:
        if not isinstance(item, dict):
            continue

        if is_novel_format(item.get("format")):
            continue

        if is_doujinshi(item):
            continue

        if title_match_score(item, query) == 0:
            continue

        item_id = item.get("id")

        if item_id and item_id in seen_ids:
            continue

        if item_id:
            seen_ids.add(item_id)

        filtered.append(item)

    return filtered


# ==================================================
# NORMALIZE ANILIST ITEM
# ==================================================

def normalize_anilist_item(item: dict) -> dict:
    title = item.get("title") or {}
    cover = item.get("coverImage") or {}

    return {
        "id": f"anilist-{item.get('id')}",
        "source": "AniList",

        "title": {
            "english": safe_text(title.get("english")),
            "romaji": safe_text(title.get("romaji")),
            "native": safe_text(title.get("native")),
        },

        "description": safe_text(item.get("description")),

        "cover": (
            cover.get("extraLarge")
            or cover.get("large")
        ),

        "rating": safe_number(item.get("averageScore")),
        "popularity": safe_number(item.get("popularity")),
        "trending": safe_number(item.get("trending")),

        "status": item.get("status"),
        "chapters": item.get("chapters"),
        "volumes": item.get("volumes"),

        "genres": normalize_genres(item.get("genres")),

        "country": item.get("countryOfOrigin"),
        "release_date": item.get("startDate"),

        "format": safe_text(item.get("format")).upper(),
        "url": item.get("siteUrl"),
    }


# ==================================================
# ANILIST SEARCH
# ==================================================

async def search_anilist(client, query, limit):

    response = await client.post(
        ANILIST_URL,
        json={
            "query": ANILIST_QUERY,
            "variables": {
                "search": query,
                "perPage": limit
            },
        },
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

        if is_novel_format(item.get("format")):
            continue

        results.append(normalize_anilist_item(item))

    return results


# ==================================================
# MANGADEX SEARCH
# ==================================================

async def search_mangadex(client, query, limit):

    params = [
        ("title", query),
        ("limit", limit),
        ("includes[]", "cover_art"),
        ("contentRating[]", "safe"),
        ("contentRating[]", "suggestive"),
        ("order[relevance]", "desc"),
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
                    relation.get("attributes", {}).get("fileName")
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

        genres = normalize_genres([
            tag.get("attributes", {})
            .get("name", {})
            .get("en")
            for tag in attributes.get("tags", [])
        ])

        results.append({
            "id": f"mangadex-{item['id']}",
            "source": "MangaDex",

            "title": {
                "english": safe_text(english_title),
                "romaji": safe_text(
                    next(iter(titles.values()), "")
                ),
                "native": safe_text(native_title),
            },

            "description": safe_text(
                (attributes.get("description") or {}).get("en")
            ),

            "cover": cover_url,
            "rating": None,
            "popularity": None,

            "status": attributes.get("status"),
            "chapters": None,

            "genres": genres,
            "country": attributes.get("originalLanguage"),
            "release_date": attributes.get("year"),

            "format": None,

            "url": (
                f"https://mangadex.org/title/{item['id']}"
            ),
        })

    return results


# ==================================================
# MANGABAKA SEARCH
# ==================================================

async def search_mangabaka(client, query, limit):

    params = [
        ("q", query),
        ("limit", limit),
        ("content_rating", "safe"),
        ("content_rating", "suggestive"),
    ]

    response = await client.get(
        f"{MANGABAKA_URL}/series/search",
        params=params
    )

    response.raise_for_status()
    payload = response.json()

    status = payload.get("status")

    if status not in (
        None, 200, "200", "ok", "success"
    ):
        raise ValueError(
            f"MangaBaka returned status: {status}"
        )

    results = []

    for item in payload.get("data", []):

        if not isinstance(item, dict):
            continue

        manga_format = safe_text(
            item.get("format") or item.get("type")
        )

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

        native = japanese or korean or chinese

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
                "romaji": "",
                "native": native,
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
            ),
        })

    return results


# ==================================================
# MERGE RESULTS
# ==================================================

def merge_results(results: list[dict]) -> list[dict]:

    unique = []
    source_ids = set()

    for item in results:

        source = item.get("source")
        item_id = item.get("id")

        source_key = (source, item_id)

        if item_id and source_key in source_ids:
            continue

        if item_id:
            source_ids.add(source_key)

        names = {
            normalize_name(value)
            for value in title_values(item)
            if normalize_name(value)
        }

        matched = None

        if names:
            for existing in unique:

                if existing.get("source") == source:
                    continue

                existing_names = {
                    normalize_name(value)
                    for value in title_values(existing)
                    if normalize_name(value)
                }

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

        if source and source != matched.get("source"):

            if source not in matched["other_sources"]:
                matched["other_sources"].append(source)

        for field in (
            "description",
            "cover",
            "rating",
            "popularity",
            "status",
            "chapters",
            "country",
            "release_date",
            "format",
            "url",
        ):

            if (
                matched.get(field) in (None, "")
                and item.get(field) not in (None, "")
            ):
                matched[field] = item[field]

        for language in (
            "english",
            "romaji",
            "native"
        ):

            if not matched["title"].get(language):
                matched["title"][language] = (
                    (item.get("title") or {}).get(language)
                    or ""
                )

        matched["genres"] = list(dict.fromkeys(
            (matched.get("genres") or [])
            + (item.get("genres") or [])
        ))

    return unique


# ==================================================
# SORTING
# ==================================================

def calculate_relevance(item: dict, query: str) -> int:
    return title_match_score(item, query)


def sort_results(
    results: list[dict],
    query: str
) -> list[dict]:

    return sorted(
        results,
        key=lambda item: (
            calculate_relevance(item, query),
            safe_number(item.get("rating")) or 0,
            safe_number(item.get("popularity")) or 0,
        ),
        reverse=True,
    )


# ==================================================
# BASIC ROUTES
# ==================================================

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
            "catalog_discovery",
            "chapter_language_detection",
            "multi_source_search",
            "result_merging",
            "novel_filtering",
            "doujinshi_filtering",
            "strict_title_matching",
            "search_relevance",
        ],
    }


@app.get("/health")
async def health():

    return {
        "status": "healthy",
        "version": APP_VERSION
    }


# ==================================================
# CATALOG SEARCH
# ==================================================

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
    ),
):

    query = q.strip()

    if not query:
        return {
            "query": q,
            "total": 0,
            "sources_checked": [
                "AniList",
                "MangaDex",
                "MangaBaka"
            ],
            "sources_successful": [],
            "sources_with_errors": [],
            "results": [],
        }

    headers = {
        "User-Agent": f"MangaAITranslator/{APP_VERSION}",
        "Accept": "application/json",
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

            search_anilist(client, query, limit),
            search_mangadex(client, query, limit),
            search_mangabaka(client, query, limit),

            return_exceptions=True,
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

    relevant_results = filter_search_results(
        results,
        query
    )

    final_results = merge_results(
        relevant_results
    )

    final_results = sort_results(
        final_results,
        query
    )[:limit]

    return {
        "query": query,
        "total": len(final_results),

        "sources_checked": source_names,
        "sources_successful": successful_sources,
        "sources_with_errors": errors,

        "results": final_results,
    }


# ==================================================
# CATALOG DISCOVERY
# ==================================================

@app.get("/api/v1/catalog/discover")
async def catalog_discover(

    category: str = Query(
        "popular",
        description=(
            "popular, top_rated, trending, "
            "new, completed, ongoing"
        )
    ),

    page: int = Query(
        1,
        ge=1,
        le=100
    ),

    limit: int = Query(
        20,
        ge=1,
        le=50
    ),
):

    category = category.strip().lower()

    categories = {

        "popular": {
            "sort": ["POPULARITY_DESC"],
            "status": None,
        },

        "top_rated": {
            "sort": [
                "SCORE_DESC",
                "POPULARITY_DESC"
            ],
            "status": None,
        },

        "trending": {
            "sort": ["TRENDING_DESC"],
            "status": None,
        },

        "new": {
            "sort": ["START_DATE_DESC"],
            "status": None,
        },

        "completed": {
            "sort": ["POPULARITY_DESC"],
            "status": "FINISHED",
        },

        "ongoing": {
            "sort": ["POPULARITY_DESC"],
            "status": "RELEASING",
        },
    }

    if category not in categories:

        return {
            "error": "Invalid category",

            "available_categories": list(
                categories.keys()
            )
        }

    selected = categories[category]

    variables = {
        "page": page,
        "perPage": limit,
        "sort": selected["sort"],
        "status": selected["status"],
    }

    headers = {
        "User-Agent": f"MangaAITranslator/{APP_VERSION}",
        "Accept": "application/json",
    }

    try:

        async with httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(30.0),
            follow_redirects=True
        ) as client:

            response = await client.post(
                ANILIST_URL,
                json={
                    "query": DISCOVER_QUERY,
                    "variables": variables,
                },
            )

            response.raise_for_status()
            payload = response.json()

        if payload.get("errors"):

            logger.error(
                "AniList discovery errors: %s",
                payload["errors"]
            )

            return {
                "error": "AniList discovery failed",
                "category": category,
                "results": [],
            }

        page_data = (
            payload.get("data", {})
            .get("Page", {})
        )

        media = page_data.get("media", [])

        page_info = page_data.get(
            "pageInfo",
            {}
        )

        results = []

        for item in media:

            if item.get("isAdult"):
                continue

            if is_novel_format(item.get("format")):
                continue

            normalized = normalize_anilist_item(item)

            if is_doujinshi(normalized):
                continue

            results.append(normalized)

        return {

            "category": category,

            "page": page,
            "limit": limit,

            "source": "AniList",

            "total": page_info.get("total", 0),

            "has_next_page": page_info.get(
                "hasNextPage",
                False
            ),

            "results": results,
        }

    except Exception as error:

        logger.exception(
            "Catalog discovery failed"
        )

        return {
            "error": str(error),
            "category": category,
            "page": page,
            "results": [],
        }
