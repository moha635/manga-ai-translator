import asyncio
import logging
from typing import Any

import httpx
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from chapters import router as chapters_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("manga-ai")

app = FastAPI(
    title="Manga AI Translator",
    version="4.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Chapter and language detection routes
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


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


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
        if "official" in item.get("traits", []):
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
            "rating": item.get("averageScore"),
            "popularity": item.get("popularity"),
            "status": item.get("status"),
            "chapters": item.get("chapters"),
            "genres": item.get("genres") or [],
            "country": item.get("countryOfOrigin"),
            "release_date": item.get("startDate"),
            "format": item.get("format"),
            "url": item.get("siteUrl")
        })

    return results


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
            "genres": [
                tag.get("attributes", {})
                .get("name", {})
                .get("en")
                for tag in attributes.get("tags", [])
                if tag.get("attributes", {})
                .get("name", {})
                .get("en")
            ],
            "country": attributes.get("originalLanguage"),
            "release_date": attributes.get("year"),
            "format": None,
            "url": f"https://mangadex.org/title/{item['id']}"
        })

    return results


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

        rating = safe_text(item.get("content_rating")).lower()

        # Extra safety filter
        if rating not in ("safe", "suggestive"):
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
            "rating": item.get("rating"),
            "popularity": item.get("popularity"),
            "status": item.get("status"),
            "chapters": item.get("total_chapters"),
            "genres": [],
            "country": None,
            "release_date": item.get("published"),
            "format": item.get("type"),
            "url": f"https://mangabaka.org/series/{series_id}"
        })

    return results


def normalize_name(value):
    return " ".join(
        safe_text(value).casefold().split()
    )


def merge_results(results):
    unique = {}

    for item in results:
        titles = item.get("title") or {}

        possible_names = [
            titles.get("english"),
            titles.get("romaji"),
            titles.get("native")
        ]

        names = [
            normalize_name(name)
            for name in possible_names
            if normalize_name(name)
        ]

        key = names[0] if names else item["id"]

        existing_key = next(
            (
                current_key
                for current_key, existing in unique.items()
                if key == current_key
                or key in [
                    normalize_name(value)
                    for value in (existing.get("title") or {}).values()
                    if normalize_name(value)
                ]
                or any(
                    name in [
                        normalize_name(value)
                        for value in (existing.get("title") or {}).values()
                        if normalize_name(value)
                    ]
                    for name in names
                )
            ),
            None
        )

        if existing_key is None:
            item["other_sources"] = []
            unique[key] = item
            continue

        existing = unique[existing_key]

        if item["source"] != existing["source"]:
            if item["source"] not in existing["other_sources"]:
                existing["other_sources"].append(item["source"])

        # Fill missing metadata without replacing existing values
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
            "url"
        ):
            if not existing.get(field) and item.get(field):
                existing[field] = item[field]

        for language in ("english", "romaji", "native"):
            if not existing["title"].get(language):
                existing["title"][language] = (
                    item["title"].get(language)
                )

        existing["genres"] = list(dict.fromkeys(
            (existing.get("genres") or [])
            + (item.get("genres") or [])
        ))

    return list(unique.values())


@app.get("/")
async def home():
    return {
        "name": "Manga AI Translator",
        "version": "4.1.0",
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
            "result_merging"
        ]
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "version": "4.1.0"
    }


@app.get("/api/v1/catalog/search")
async def catalog_search(
    q: str = Query(..., min_length=1, max_length=100),
    limit: int = Query(10, ge=1, le=20)
):
    headers = {
        "User-Agent": "MangaAITranslator/4.1",
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

    return {
        "query": q,
        "total": len(final_results),
        "sources_checked": source_names,
        "sources_successful": successful_sources,
        "sources_with_errors": errors,
        "results": final_results
    }
