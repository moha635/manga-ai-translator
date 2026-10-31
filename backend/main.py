import asyncio
import logging
import re
import unicodedata
from typing import Any

import httpx
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from chapters import router as chapters_router


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("manga-ai")

APP_VERSION = "4.6.2"

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


# --------------------------------------------------
# SEARCH QUERY
# --------------------------------------------------

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
    fmt = safe_text(value).casefold()
    return "novel" in fmt


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
    genre_text = " ".join(
        normalize_genres(item.get("genres"))
    ).casefold()

    return (
        "doujinshi" in title_text
        or "doujinshi" in format_text
        or "doujinshi" in genre_text
    )


# --------------------------------------------------
# ARABIC SEARCH
# --------------------------------------------------

ARABIC_TO_LATIN = {
    "ا": "a", "أ": "a", "إ": "i", "آ": "aa",
    "ب": "b", "ت": "t", "ث": "th",
    "ج": "j", "ح": "h", "خ": "kh",
    "د": "d", "ذ": "dh", "ر": "r",
    "ز": "z", "س": "s", "ش": "sh",
    "ص": "s", "ض": "d", "ط": "t",
    "ظ": "z", "ع": "a", "غ": "gh",
    "ف": "f", "ق": "q", "ك": "k",
    "ل": "l", "م": "m", "ن": "n",
    "ه": "h", "ة": "a", "و": "w",
    "ؤ": "w", "ي": "y", "ى": "a",
    "ئ": "y", "ء": "", "پ": "p",
    "چ": "ch", "ڤ": "v", "گ": "g"
}


def arabic_phonetic_query(query: str) -> str:
    output = []

    for char in unicodedata.normalize("NFKC", query):
        if char in ARABIC_TO_LATIN:
            output.append(ARABIC_TO_LATIN[char])

        elif char in "ًٌٍَُِّْـٰ":
            continue

        else:
            output.append(char)

    text = "".join(output).casefold()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


ARABIC_TITLE_ALIASES = {
    "سولو ليفيلينغ": "Solo Leveling",
    "سولو ليفلينغ": "Solo Leveling",
    "سولو ليفلنج": "Solo Leveling",
    "سولو ليفلینگ": "Solo Leveling",
}


def search_query_variants(query: str) -> list[str]:
    variants = [query.strip()]

    normalized = normalize_name(query)
    known_alias = ARABIC_TITLE_ALIASES.get(normalized)

    if known_alias:
        variants.append(known_alias)

    if re.search(r"[\u0600-\u06ff]", query):
        phonetic = arabic_phonetic_query(query)

        if (
            phonetic
            and normalize_name(phonetic) != normalize_name(query)
        ):
            variants.append(phonetic)

    return list(dict.fromkeys(
        value for value in variants if value
    ))


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

    variants = search_query_variants(query)

    for item in results:
        if not isinstance(item, dict):
            continue

        if is_novel_format(item.get("format")):
            continue

        if is_doujinshi(item):
            continue

        if not any(
            title_match_score(item, candidate) > 0
            for candidate in variants
        ):
            continue

        item_id = item.get("id")

        if item_id and item_id in seen_ids:
            continue

        if item_id:
            seen_ids.add(item_id)

        filtered.append(item)

    return filtered


# --------------------------------------------------
# ANILIST SEARCH
# --------------------------------------------------

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
        raise ValueError(
            f"AniList GraphQL error: {payload['errors']}"
        )

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

        title = item.get("title") or {}
        cover = item.get("coverImage") or {}

        results.append({
            "id": f"anilist-{item['id']}",
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
            "status": item.get("status"),
            "chapters": item.get("chapters"),

            "genres": normalize_genres(item.get("genres")),

            "country": item.get("countryOfOrigin"),
            "release_date": item.get("startDate"),

            "format": safe_text(item.get("format")).upper(),
            "url": item.get("siteUrl"),
        })

    return results


# --------------------------------------------------
# MANGADEX SEARCH
# --------------------------------------------------

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


# --------------------------------------------------
# MANGABAKA SEARCH
# --------------------------------------------------

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

            "rating": safe_number(item.get("rating")),

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


# --------------------------------------------------
# MERGE RESULTS
# --------------------------------------------------

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
            "url"
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


# --------------------------------------------------
# RELEVANCE AND SORTING
# --------------------------------------------------

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


# --------------------------------------------------
# HOME AND HEALTH
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
            "doujinshi_filtering",
            "strict_title_matching",
            "search_relevance",
            "arabic_phonetic_search_variants",
            "multi_query_search",
            "catalog_discovery",
            "popular_catalog",
            "top_rated_catalog",
            "trending_catalog",
            "completed_catalog",
            "ongoing_catalog",
        ],
    }


@app.get("/health")
async def health():

    return {
        "status": "healthy",
        "version": APP_VERSION
    }


# --------------------------------------------------
# CATALOG DISCOVERY
# --------------------------------------------------

DISCOVER_QUERY = """
query (
  $page: Int!,
  $perPage: Int!,
  $sort: [MediaSort],
  $status: MediaStatus,
  $formats: [MediaFormat]
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
      format_in: $formats,
      isAdult: false
    ) {

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
      meanScore
      popularity
      trending
      favourites
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

      endDate {
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


DISCOVER_SORTS = {
    "popular": [
        "POPULARITY_DESC",
        "SCORE_DESC"
    ],

    "top_rated": [
        "SCORE_DESC",
        "POPULARITY_DESC"
    ],

    "trending": [
        "TRENDING_DESC",
        "POPULARITY_DESC"
    ],

    "new": [
        "START_DATE_DESC",
        "POPULARITY_DESC"
    ],

    "completed": [
        "POPULARITY_DESC",
        "SCORE_DESC"
    ],

    "ongoing": [
        "POPULARITY_DESC",
        "SCORE_DESC"
    ],
}


def normalize_discovery_item(item: dict) -> dict:

    title = item.get("title") or {}
    cover = item.get("coverImage") or {}

    start = item.get("startDate") or {}
    end = item.get("endDate") or {}

    english = safe_text(title.get("english"))
    romaji = safe_text(title.get("romaji"))
    native = safe_text(title.get("native"))

    display_title = (
        english
        or romaji
        or native
        or "Untitled"
    )

    return {
        "id": f"anilist-{item.get('id')}",
        "source": "AniList",

        "title": {
            "english": english,
            "romaji": romaji,
            "native": native,
        },

        "display_title": display_title,

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

        "mean_rating": safe_number(
            item.get("meanScore")
        ),

        "popularity": safe_number(
            item.get("popularity")
        ),

        "trending": safe_number(
            item.get("trending")
        ),

        "favourites": safe_number(
            item.get("favourites")
        ),

        "status": (
            safe_text(item.get("status")).upper()
            or None
        ),

        "chapters": item.get("chapters"),
        "volumes": item.get("volumes"),

        "genres": normalize_genres(
            item.get("genres")
        ),

        "country": item.get("countryOfOrigin"),

        "release_date": start,
        "end_date": end,

        "format": safe_text(
            item.get("format")
        ).upper(),

        "url": item.get("siteUrl"),

        "other_sources": [],
    }


@app.get("/api/v1/catalog/discover")
async def catalog_discover(

    category: str = Query(
        "popular",
        pattern="^(popular|top_rated|trending|new|completed|ongoing)$"
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

    variables = {
        "page": page,
        "perPage": limit,
        "sort": DISCOVER_SORTS[category],
        "formats": [
            "MANGA",
            "ONE_SHOT"
        ],
    }

    # Send status only for categories that need it.
    # Never send null values to comparison filters.

    if category == "completed":
        variables["status"] = "FINISHED"

    elif category == "ongoing":
        variables["status"] = "RELEASING"

    headers = {
        "User-Agent": f"MangaAITranslator/{APP_VERSION}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    timeout = httpx.Timeout(25.0)

    try:

        async with httpx.AsyncClient(
            headers=headers,
            timeout=timeout
        ) as client:

            response = await client.post(
                ANILIST_URL,
                json={
                    "query": DISCOVER_QUERY,
                    "variables": variables
                }
            )

            if response.is_error:

                logger.error(
                    "AniList discovery HTTP error: %s - %s",
                    response.status_code,
                    response.text[:2000]
                )

                raise HTTPException(
                    status_code=502,
                    detail={
                        "message": "AniList discovery request failed",
                        "upstream_status": response.status_code,
                        "upstream_response": response.text[:1000],
                    }
                )

            payload = response.json()

    except httpx.TimeoutException:

        logger.exception(
            "AniList discovery timed out"
        )

        raise HTTPException(
            status_code=504,
            detail="AniList request timed out"
        )

    except httpx.RequestError:

        logger.exception(
            "AniList discovery connection error"
        )

        raise HTTPException(
            status_code=502,
            detail="Could not connect to AniList"
        )

    if payload.get("errors"):

        logger.error(
            "AniList discovery GraphQL errors: %s",
            payload["errors"]
        )

        raise HTTPException(
            status_code=502,
            detail={
                "message": "AniList GraphQL query failed",
                "errors": payload["errors"],
            }
        )

    page_data = (
        (payload.get("data") or {}).get("Page")
        or {}
    )

    info = page_data.get("pageInfo") or {}
    media = page_data.get("media") or []

    results = []

    for item in media:

        if not isinstance(item, dict):
            continue

        if item.get("isAdult"):
            continue

        if is_novel_format(item.get("format")):
            continue

        if is_doujinshi(item):
            continue

        normalized = normalize_discovery_item(item)

        popularity = normalized["popularity"] or 0
        rating = normalized["rating"] or 0

        if category == "popular":
            if popularity <= 1000:
                continue

        if category == "top_rated":
            if rating <= 70 or popularity <= 500:
                continue

        results.append(normalized)

    # Defensive sorting.

    sort_key = {

        "popular": lambda x: (
            x["popularity"] or 0,
            x["rating"] or 0
        ),

        "top_rated": lambda x: (
            x["rating"] or 0,
            x["popularity"] or 0
        ),

        "trending": lambda x: (
            x["trending"] or 0,
            x["popularity"] or 0
        ),

        "new": lambda x: (
            (x["release_date"] or {}).get("year") or 0,
            (x["release_date"] or {}).get("month") or 0,
            (x["release_date"] or {}).get("day") or 0
        ),

        "completed": lambda x: (
            x["popularity"] or 0,
            x["rating"] or 0
        ),

        "ongoing": lambda x: (
            x["popularity"] or 0,
            x["rating"] or 0
        ),
    }

    results.sort(
        key=sort_key[category],
        reverse=True
    )

    return {
        "category": category,
        "page": page,
        "limit": limit,
        "source": "AniList",
        "total": info.get("total", len(results)),
        "has_next_page": bool(
            info.get("hasNextPage")
        ),
        "results": results,
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

        variants = search_query_variants(query)

        tasks = []
        task_labels = []

        for candidate in variants:

            tasks.extend([
                search_anilist(
                    client,
                    candidate,
                    limit
                ),

                search_mangadex(
                    client,
                    candidate,
                    limit
                ),

                search_mangabaka(
                    client,
                    candidate,
                    limit
                ),
            ])

            task_labels.extend([
                (name, candidate)
                for name in source_names
            ])

        responses = await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

    results = []
    errors = []
    successful_sources = []

    for (name, candidate), response in zip(
        task_labels,
        responses
    ):

        if isinstance(response, Exception):

            logger.warning(
                "%s failed for query variant %r: %s",
                name,
                candidate,
                response
            )

            if name not in errors:
                errors.append(name)

        else:

            if name not in successful_sources:
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

        "query_variants": search_query_variants(
            query
        ),

        "total": len(final_results),

        "sources_checked": source_names,

        "sources_successful": successful_sources,

        "sources_with_errors": errors,

        "results": final_results,
    }
