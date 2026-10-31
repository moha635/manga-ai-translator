import asyncio
import logging
from typing import Any

import httpx
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("manga-ai")

app = FastAPI(
    title="Manga AI Translator",
    version="3.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


ANILIST_URL = "https://graphql.anilist.co"
MANGADEX_URL = "https://api.mangadex.org"


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


def normalize_result(item: dict) -> dict:
    titles = item.get("title") or {}

    return {
        "id": item.get("id"),
        "source": item.get("source"),
        "title": titles,
        "description": item.get("description", ""),
        "cover": item.get("cover"),
        "rating": item.get("rating"),
        "popularity": item.get("popularity"),
        "status": item.get("status"),
        "chapters": item.get("chapters"),
        "genres": item.get("genres", []),
        "country": item.get("country"),
        "release_date": item.get("release_date"),
        "format": item.get("format"),
        "url": item.get("url"),
    }


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
                f"https://uploads.mangadex.org/covers/"
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


@app.get("/")
async def home():
    return {
        "name": "Manga AI Translator",
        "version": "3.0.0",
        "status": "running",
        "sources": ["AniList", "MangaDex"]
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/api/v1/catalog/search")
async def catalog_search(
    q: str = Query(..., min_length=1, max_length=100),
    limit: int = Query(10, ge=1, le=20)
):
    headers = {
        "User-Agent": "MangaAITranslator/3.0",
        "Accept": "application/json"
    }

    timeout = httpx.Timeout(25.0)

    async with httpx.AsyncClient(
        headers=headers,
        timeout=timeout
    ) as client:

        responses = await asyncio.gather(
            search_anilist(client, q, limit),
            search_mangadex(client, q, limit),
            return_exceptions=True
        )

    results = []
    errors = []

    for name, response in zip(
        ["AniList", "MangaDex"],
        responses
    ):
        if isinstance(response, Exception):
            logger.warning("%s failed: %s", name, response)
            errors.append(name)
        else:
            results.extend(response)

    unique = {}

    for item in results:
        titles = item.get("title") or {}

        possible_names = [
            titles.get("english"),
            titles.get("romaji"),
            titles.get("native")
        ]

        key = next(
            (
                name.casefold().strip()
                for name in possible_names
                if name and name.strip()
            ),
            item["id"]
        )

        if key not in unique:
            unique[key] = item
            unique[key]["other_sources"] = []
        else:
            unique[key]["other_sources"].append(
                item["source"]
            )

    final_results = list(unique.values())

    return {
        "query": q,
        "total": len(final_results),
        "sources_checked": ["AniList", "MangaDex"],
        "sources_with_errors": errors,
        "results": final_results
    }
