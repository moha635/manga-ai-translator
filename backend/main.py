import asyncio
import logging
import re
import unicodedata
import calendar
import math
import time

from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Optional

import httpx

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from chapters import router as chapters_router


# ==================================================
# CONFIGURATION
# ==================================================

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("manga-ai")

APP_VERSION = "4.8.0"

ANILIST_URL = "https://graphql.anilist.co"
MANGADEX_URL = "https://api.mangadex.org"
MANGABAKA_URL = "https://api.mangabaka.org/v2"

REQUEST_TIMEOUT = 25.0

DISCOVERY_SCAN_LIMIT = 20
DISCOVERY_PAGE_SIZE = 50
MAX_RAW_ITEMS = 1000

NEW_CATALOG_MONTHS = 18


# ==================================================
# HTTP RETRY CONFIGURATION
# ==================================================

HTTP_RETRY_ATTEMPTS = 2
HTTP_RETRY_BACKOFF = 0.6
MAX_RETRY_AFTER = 30

RETRYABLE_STATUS_CODES = {
    429,
    500,
    502,
    503,
    504,
}


# ==================================================
# CACHE CONFIGURATION
# ==================================================

SEARCH_CACHE_TTL = 300
SEARCH_CACHE_MAX_SIZE = 500

DISCOVERY_CACHE_TTL = 600
DISCOVERY_CACHE_MAX_SIZE = 200


# ==================================================
# SHARED HTTP CLIENT
# ==================================================

_http_client: Optional[httpx.AsyncClient] = None


def get_client() -> httpx.AsyncClient:

    if _http_client is None or _http_client.is_closed:
        raise RuntimeError(
            "HTTP client is not initialized"
        )

    return _http_client


@asynccontextmanager
async def lifespan(app: FastAPI):

    global _http_client

    headers = {
        "User-Agent": f"MangaAITranslator/{APP_VERSION}",
        "Accept": "application/json",
    }

    timeout = httpx.Timeout(
        REQUEST_TIMEOUT
    )

    _http_client = httpx.AsyncClient(
        headers=headers,
        timeout=timeout,
        follow_redirects=True,
    )

    logger.info(
        "Manga AI Translator %s started",
        APP_VERSION
    )

    try:
        yield

    finally:

        if _http_client is not None:
            await _http_client.aclose()
            _http_client = None

        logger.info(
            "Manga AI Translator stopped"
        )


# ==================================================
# FASTAPI APPLICATION
# ==================================================

app = FastAPI(
    title="Manga AI Translator",
    version=APP_VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    chapters_router
)


# ==================================================
# TTL CACHE
# ==================================================

class TTLCache:

    def __init__(
        self,
        ttl: int,
        max_size: int
    ):

        self.ttl = ttl
        self.max_size = max_size

        self._data = {}
        self._lock = asyncio.Lock()

    async def get(self, key):

        async with self._lock:

            entry = self._data.get(key)

            if entry is None:
                return None

            expires_at, value = entry

            if time.monotonic() >= expires_at:

                self._data.pop(
                    key,
                    None
                )

                return None

            return value

    async def set(
        self,
        key,
        value
    ):

        async with self._lock:

            now = time.monotonic()

            expired = [
                cache_key
                for cache_key, entry in self._data.items()
                if now >= entry[0]
            ]

            for cache_key in expired:

                self._data.pop(
                    cache_key,
                    None
                )

            if key in self._data:

                self._data[key] = (
                    now + self.ttl,
                    value
                )

                return

            if len(self._data) >= self.max_size:

                remove_count = max(
                    1,
                    self.max_size // 4
                )

                oldest = sorted(
                    self._data.items(),
                    key=lambda entry: entry[1][0]
                )

                for cache_key, _ in oldest[:remove_count]:

                    self._data.pop(
                        cache_key,
                        None
                    )

            self._data[key] = (
                now + self.ttl,
                value
            )

    async def clear(self):

        async with self._lock:
            self._data.clear()


# ==================================================
# CACHE INSTANCES
# ==================================================

search_cache = TTLCache(
    ttl=SEARCH_CACHE_TTL,
    max_size=SEARCH_CACHE_MAX_SIZE
)

discovery_cache = TTLCache(
    ttl=DISCOVERY_CACHE_TTL,
    max_size=DISCOVERY_CACHE_MAX_SIZE
)


# ==================================================
# RETRY-AFTER PARSER
# ==================================================

def parse_retry_after(
    value: Optional[str]
) -> Optional[float]:

    if not value:
        return None

    value = value.strip()

    try:

        seconds = float(value)

        if not math.isfinite(seconds):
            return None

        return max(
            0.0,
            seconds
        )

    except (ValueError, TypeError):
        pass

    try:

        retry_date = parsedate_to_datetime(
            value
        )

        if retry_date.tzinfo is None:

            retry_date = retry_date.replace(
                tzinfo=timezone.utc
            )

        now = datetime.now(
            timezone.utc
        )

        seconds = (
            retry_date - now
        ).total_seconds()

        return max(
            0.0,
            seconds
        )

    except (
        TypeError,
        ValueError,
        OverflowError
    ):

        return None


# ==================================================
# RETRY BACKOFF
# ==================================================

async def _sleep_backoff(
    attempt: int,
    retry_after: Optional[float] = None
):

    if retry_after is not None:

        if retry_after > MAX_RETRY_AFTER:

            logger.warning(
                "Retry-After %.2f exceeds maximum %s seconds; capped.",
                retry_after,
                MAX_RETRY_AFTER
            )

        delay = min(
            retry_after,
            MAX_RETRY_AFTER
        )

    else:

        delay = min(
            HTTP_RETRY_BACKOFF * (2 ** attempt),
            MAX_RETRY_AFTER
        )

    await asyncio.sleep(delay)


# ==================================================
# HTTP REQUEST WITH RETRIES
# ==================================================

async def http_request(
    method: str,
    url: str,
    **kwargs
):

    client = get_client()

    last_exception = None

    for attempt in range(
        HTTP_RETRY_ATTEMPTS + 1
    ):

        response = None

        try:

            request_method = method.casefold()

            if request_method == "get":

                response = await client.get(
                    url,
                    **kwargs
                )

            elif request_method == "post":

                response = await client.post(
                    url,
                    **kwargs
                )

            else:

                response = await client.request(
                    method,
                    url,
                    **kwargs
                )

            try:

                response.raise_for_status()

            except httpx.HTTPStatusError:

                status = response.status_code

                if (
                    status not in RETRYABLE_STATUS_CODES
                    or attempt >= HTTP_RETRY_ATTEMPTS
                ):

                    raise

                retry_after = parse_retry_after(
                    response.headers.get("Retry-After")
                )

                logger.warning(
                    "HTTP status %s; retrying (%s/%s)",
                    status,
                    attempt + 1,
                    HTTP_RETRY_ATTEMPTS
                )

                await response.aclose()
                response = None

                await _sleep_backoff(
                    attempt,
                    retry_after
                )

                continue

            return response

        except httpx.RequestError as exc:

            last_exception = exc

            if response is not None:

                await response.aclose()
                response = None

            if attempt >= HTTP_RETRY_ATTEMPTS:
                raise

            logger.warning(
                "HTTP request failed (%s/%s): %s",
                attempt + 1,
                HTTP_RETRY_ATTEMPTS,
                exc
            )

            await _sleep_backoff(
                attempt
            )

        except httpx.HTTPStatusError as exc:

            last_exception = exc

            if response is not None:

                await response.aclose()
                response = None

            raise exc

    if last_exception is not None:
        raise last_exception

    raise RuntimeError(
        "HTTP request failed unexpectedly"
    )


# ==================================================
# GENERAL HELPERS
# ==================================================

def safe_text(value: Any) -> str:

    if value is None or isinstance(
        value,
        (dict, list)
    ):
        return ""

    return str(value).strip()


def safe_number(value: Any):

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):

        number = float(value)

        if not math.isfinite(number):
            return None

        return number

    if isinstance(value, str):

        try:

            number = float(value)

            if not math.isfinite(number):
                return None

        except ValueError:
            return None

        return number

    return None


def popularity_number(value: Any) -> float:

    direct = safe_number(value)

    if direct is not None:
        return direct

    if isinstance(value, dict):

        for key in (
            "score",
            "value",
            "popularity"
        ):

            number = safe_number(
                value.get(key)
            )

            if number is not None:
                return number

    return 0.0


def normalize_name(value: Any) -> str:

    text = safe_text(value).casefold()

    text = unicodedata.normalize(
        "NFKC",
        text
    )

    text = re.sub(
        r"[^\w\s]",
        " ",
        text
    )

    return " ".join(
        text.split()
    )


def title_values(item: dict) -> list[str]:

    titles = item.get("title") or {}

    if isinstance(titles, str):

        values = [titles]

    elif isinstance(titles, dict):

        values = list(
            titles.values()
        )

    else:

        values = []

    return list(dict.fromkeys(
        safe_text(value)
        for value in values
        if safe_text(value)
    ))


def first_title(
    titles,
    language="en"
):

    if isinstance(titles, str):
        return titles

    if not isinstance(titles, list):
        return ""

    matching = [
        item
        for item in titles
        if isinstance(item, dict)
        and item.get("language") == language
    ]

    for item in matching:

        if item.get("is_primary"):

            return safe_text(
                item.get("title")
            )

    for item in matching:

        traits = item.get("traits") or []

        if (
            isinstance(traits, list)
            and "official" in traits
        ):

            return safe_text(
                item.get("title")
            )

    if matching:

        return safe_text(
            matching[0].get("title")
        )

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

    fmt = safe_text(
        value
    ).casefold()

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

    title_text = " ".join(
        title_values(item)
    ).casefold()

    format_text = safe_text(
        item.get("format")
    ).casefold()

    genre_text = " ".join(
        normalize_genres(
            item.get("genres")
        )
    ).casefold()

    return (
        "doujinshi" in title_text
        or "doujinshi" in format_text
        or "doujinshi" in genre_text
    )


def usable_display_title(item: dict) -> str:

    titles = item.get("title") or {}

    if isinstance(titles, str):

        return safe_text(
            titles
        ) or "Untitled"

    if not isinstance(titles, dict):
        return "Untitled"

    return (
        safe_text(titles.get("english"))
        or safe_text(titles.get("romaji"))
        or safe_text(titles.get("native"))
        or "Untitled"
    )


def normalize_date(value: Any) -> dict:

    if not isinstance(value, dict):

        return {
            "year": None,
            "month": None,
            "day": None
        }

    return {
        "year": value.get("year"),
        "month": value.get("month"),
        "day": value.get("day")
    }


# ==================================================
# RELEASE DATE HELPERS
# ==================================================

def release_date_timestamp(value: Any) -> Optional[int]:

    if not isinstance(value, dict):
        return None

    year = safe_number(value.get("year"))
    month = safe_number(value.get("month"))
    day = safe_number(value.get("day"))

    if not year:
        return None

    try:

        date_value = datetime(
            int(year),
            int(month or 1),
            int(day or 1),
            tzinfo=timezone.utc
        )

        return int(
            date_value.timestamp()
        )

    except (
        ValueError,
        TypeError,
        OverflowError
    ):

        return None


def release_date_text(value: Any) -> Optional[str]:

    if not isinstance(value, dict):
        return None

    year = value.get("year")
    month = value.get("month")
    day = value.get("day")

    if not year:
        return None

    try:

        year = int(year)

        if month and day:

            return (
                f"{year:04d}-"
                f"{int(month):02d}-"
                f"{int(day):02d}"
            )

        if month:

            return (
                f"{year:04d}-"
                f"{int(month):02d}"
            )

        return str(year)

    except (
        ValueError,
        TypeError,
        OverflowError
    ):

        return None


def extract_release_date(item: dict) -> Optional[str]:

    if not isinstance(item, dict):
        return None

    date_value = (
        item.get("startDate")
        or item.get("start_date")
        or item.get("releaseDate")
        or item.get("published")
    )

    if isinstance(date_value, str):

        return date_value.strip() or None

    return release_date_text(
        date_value
    )


# ==================================================
# ANILIST SEARCH
# ==================================================

ANILIST_SEARCH_QUERY = """
query ($search: String) {
    Page(page: 1, perPage: 10) {
        media(
            search: $search
            type: MANGA
            sort: SEARCH_MATCH
        ) {
            id
            idMal
            title {
                romaji
                english
                native
            }
            description(asHtml: false)
            coverImage {
                extraLarge
                large
                medium
            }
            format
            status
            chapters
            volumes
            averageScore
            popularity
            genres
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
            countryOfOrigin
            isAdult
            siteUrl
        }
    }
}
"""


async def search_anilist(
    client: httpx.AsyncClient,
    query: str
) -> list[dict]:

    if not query or not query.strip():
        return []

    response = await http_request(
        "POST",
        ANILIST_URL,
        json={
            "query": ANILIST_SEARCH_QUERY,
            "variables": {
                "search": query.strip()
            }
        },
        timeout=REQUEST_TIMEOUT
    )

    try:

        data = response.json()

    finally:

        await response.aclose()

    if not isinstance(data, dict):
        return []

    errors = data.get("errors")

    if errors:

        logger.warning(
            "AniList returned GraphQL errors: %s",
            errors
        )

    page = data.get("data", {}).get("Page", {})

    results = page.get("media", [])

    if not isinstance(results, list):
        return []

    return [
        item
        for item in results
        if isinstance(item, dict)
    ]


# ==================================================
# MANGADEX SEARCH
# ==================================================

async def search_mangadex(
    client: httpx.AsyncClient,
    query: str
) -> list[dict]:

    if not query or not query.strip():
        return []

    response = await http_request(
        "GET",
        f"{MANGADEX_URL}/manga",
        params={
            "title": query.strip(),
            "limit": 10,
            "includes[]": [
                "cover_art"
            ],
            "order[relevance]": "desc"
        },
        timeout=REQUEST_TIMEOUT
    )

    try:

        data = response.json()

    finally:

        await response.aclose()

    if not isinstance(data, dict):
        return []

    results = data.get("data", [])

    if not isinstance(results, list):
        return []

    return [
        item
        for item in results
        if isinstance(item, dict)
    ]


# ==================================================
# MANGABAKA SEARCH
# ==================================================

async def search_mangabaka(
    client: httpx.AsyncClient,
    query: str
) -> list[dict]:

    if not query or not query.strip():
        return []

    response = await http_request(
        "GET",
        MANGABAKA_URL,
        params={
            "q": query.strip()
        },
        timeout=REQUEST_TIMEOUT
    )

    try:

        data = response.json()

    finally:

        await response.aclose()

    if not isinstance(data, dict):
        return []

    results = (
        data.get("data")
        or data.get("results")
        or data.get("items")
        or []
    )

    if isinstance(results, dict):

        results = (
            results.get("items")
            or results.get("results")
            or []
        )

    if not isinstance(results, list):
        return []

    return [
        item
        for item in results
   if isinstance(item, dict)
    ]
    # ==================================================
# MERGE HELPERS
# ==================================================

def get_source_id(item: dict) -> str:

    source = safe_text(
        item.get("source")
    ).casefold()

    item_id = (
        item.get("id")
        or item.get("mal_id")
        or item.get("mangadex_id")
    )

    return f"{source}:{item_id}"


def merge_titles(
    first: dict,
    second: dict
) -> dict:

    result = {}

    for key in (
        "english",
        "romaji",
        "native"
    ):

        value = (
            safe_text(first.get(key))
            or safe_text(second.get(key))
        )

        if value:
            result[key] = value

    return result


def merge_manga_items(
    first: dict,
    second: dict
) -> dict:

    merged = dict(first)

    for key, value in second.items():

        if value is None or value == "":
            continue

        if key == "title":

            first_title_data = (
                merged.get("title")
                or {}
            )

            second_title_data = (
                value
                or {}
            )

            if (
                isinstance(first_title_data, dict)
                and isinstance(second_title_data, dict)
            ):

                merged["title"] = merge_titles(
                    first_title_data,
                    second_title_data
                )

            continue

        if key == "genres":

            existing = normalize_genres(
                merged.get("genres")
            )

            incoming = normalize_genres(
                value
            )

            merged["genres"] = list(
                dict.fromkeys(
                    existing + incoming
                )
            )

            continue

        if key not in merged or merged[key] in (
            None,
            "",
            [],
            {}
        ):

            merged[key] = value

    return merged


def merge_search_results(
    results: list[dict]
) -> list[dict]:

    merged = {}
    order = []

    for item in results:

        if not isinstance(item, dict):
            continue

        source_id = get_source_id(item)

        if source_id not in merged:

            merged[source_id] = dict(item)
            order.append(source_id)

        else:

            merged[source_id] = merge_manga_items(
                merged[source_id],
                item
            )

    return [
        merged[item_id]
        for item_id in order
    ]


# ==================================================
# SEARCH SORTING
# ==================================================

def calculate_relevance(
    item: dict,
    query: str
) -> float:

    query_normalized = normalize_name(
        query
    )

    if not query_normalized:
        return 0.0

    titles = title_values(item)

    score = 0.0

    for title in titles:

        normalized_title = normalize_name(
            title
        )

        if not normalized_title:
            continue

        if normalized_title == query_normalized:

            score = max(score, 100.0)

        elif query_normalized in normalized_title:

            score = max(score, 80.0)

        elif normalized_title in query_normalized:

            score = max(score, 65.0)

        else:

            query_words = set(
                query_normalized.split()
            )

            title_words = set(
                normalized_title.split()
            )

            if query_words and title_words:

                overlap = len(
                    query_words & title_words
                )

                word_score = (
                    overlap / len(query_words)
                ) * 50.0

                score = max(
                    score,
                    word_score
                )

    phonetic = arabic_phonetic_query(
        query
    )

    if phonetic:

        phonetic_normalized = normalize_name(
            phonetic
        )

        for title in titles:

            normalized_title = normalize_name(
                title
            )

            if (
                phonetic_normalized
                and phonetic_normalized in normalized_title
            ):

                score = max(
                    score,
                    55.0
                )

    return score


def sort_search_results(
    results: list[dict],
    query: str
) -> list[dict]:

    return sorted(
        results,
        key=lambda item: (
            calculate_relevance(
                item,
                query
            ),
            popularity_number(
                item.get("popularity")
                or item.get("averageScore")
            )
        ),
        reverse=True
    )


# ==================================================
# HOME AND HEALTH
# ==================================================

@app.get("/")
async def home():

    return {
        "name": "Manga AI Translator",
        "version": APP_VERSION,
        "status": "running"
    }


@app.get("/health")
async def health():

    return {
        "status": "healthy",
        "version": APP_VERSION
    }


# ==================================================
# DISCOVERY QUERY
# ==================================================

DISCOVER_QUERY = """
query (
    $page: Int,
    $perPage: Int,
    $sort: [MediaSort],
    $status: MediaStatus,
    $format: MediaFormat
) {
    Page(
        page: $page,
        perPage: $perPage
    ) {
        pageInfo {
            currentPage
            hasNextPage
            total
        }
        media(
            type: MANGA,
            sort: $sort,
            status: $status,
            format: $format
        ) {
            id
            idMal
            title {
                romaji
                english
                native
            }
            description(asHtml: false)
            coverImage {
                extraLarge
                large
                medium
            }
            format
            status
            chapters
            volumes
            averageScore
            popularity
            genres
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
            countryOfOrigin
            isAdult
            siteUrl
        }
    }
}
"""


# ==================================================
# DISCOVERY SORTS
# ==================================================

DISCOVER_SORTS = {
    "popular": [
        "POPULARITY_DESC"
    ],
    "trending": [
        "TRENDING_DESC"
    ],
    "score": [
        "SCORE_DESC"
    ],
    "newest": [
        "START_DATE_DESC"
    ],
    "updated": [
        "UPDATED_AT_DESC"
    ],
    "title": [
        "TITLE_ROMAJI"
    ]
}


# ==================================================
# NORMALIZE DISCOVERY ITEM
# ==================================================

def normalize_discovery_item(
    item: dict
) -> dict:

    if not isinstance(item, dict):
        return {}

    title = item.get("title") or {}

    cover = item.get("coverImage") or {}

    start_date = normalize_date(
        item.get("startDate")
    )

    end_date = normalize_date(
        item.get("endDate")
    )

    return {
        "id": item.get("id"),
        "mal_id": item.get("idMal"),
        "source": "anilist",

        "title": {
            "english": safe_text(
                title.get("english")
            ),
            "romaji": safe_text(
                title.get("romaji")
            ),
            "native": safe_text(
                title.get("native")
            )
        },

        "display_title": usable_display_title(
            item
        ),

        "description": safe_text(
            item.get("description")
        ),

        "cover": (
            cover.get("extraLarge")
            or cover.get("large")
            or cover.get("medium")
        ),

        "format": item.get("format"),
        "status": item.get("status"),

        "chapters": item.get("chapters"),
        "volumes": item.get("volumes"),

        "averageScore": item.get("averageScore"),
        "popularity": item.get("popularity"),

        "genres": normalize_genres(
            item.get("genres")
        ),

        "startDate": start_date,
        "endDate": end_date,

        "countryOfOrigin": item.get(
            "countryOfOrigin"
        ),

        "isAdult": bool(
            item.get("isAdult", False)
        ),

        "siteUrl": item.get("siteUrl")
    }


# ==================================================
# DISCOVERY STATUS HELPERS
# ==================================================

def normalize_discovery_status(value: Any) -> str:

    status = safe_text(value).upper()

    aliases = {
        "FINISHED": "FINISHED",
        "COMPLETED": "FINISHED",
        "RELEASING": "RELEASING",
        "ONGOING": "RELEASING",
        "NOT_YET_RELEASED": "NOT_YET_RELEASED",
        "UPCOMING": "NOT_YET_RELEASED",
        "CANCELLED": "CANCELLED",
        "HIATUS": "HIATUS"
    }

    return aliases.get(status, status)


# ==================================================
# DISCOVERY FILTERING
# ==================================================

def filter_discovery_items(
    items: list[dict],
    category: Optional[str] = None,
    include_adult: bool = False
) -> list[dict]:

    filtered = []

    for item in items:

        if not isinstance(item, dict):
            continue

        if (
            not include_adult
            and item.get("isAdult")
        ):
            continue

        if is_doujinshi(item):
            continue

        if category:

            item_format = safe_text(
                item.get("format")
            ).upper()

            item_status = normalize_discovery_status(
                item.get("status")
            )

            category_value = category.upper()

            if category_value in (
                "NOVEL",
                "MANGA",
                "ONE_SHOT",
                "MANHWA",
                "MANHUA"
            ):

                if item_format != category_value:
                    continue

            elif category_value in (
                "FINISHED",
                "RELEASING",
                "HIATUS",
                "CANCELLED",
                "NOT_YET_RELEASED"
            ):

                if item_status != category_value:
                    continue

        filtered.append(item)

    return filtered


# ==================================================
# DISCOVERY SORTING
# ==================================================

def sort_discovery_items(
    items: list[dict],
    sort: str = "popular"
) -> list[dict]:

    sort = safe_text(sort).casefold()

    if sort == "score":

        return sorted(
            items,
            key=lambda item: popularity_number(
                item.get("averageScore")
            ),
            reverse=True
        )

    if sort == "newest":

        return sorted(
            items,
            key=lambda item: (
                release_date_timestamp(
                    item.get("startDate")
                ) or 0
            ),
            reverse=True
        )

    if sort == "updated":

        return sorted(
            items,
            key=lambda item: popularity_number(
                item.get("updatedAt")
            ),
            reverse=True
        )

    if sort == "title":

        return sorted(
            items,
            key=lambda item: usable_display_title(
                item
            ).casefold()
        )

    if sort == "trending":

        return sorted(
            items,
            key=lambda item: popularity_number(
                item.get("trending")
                or item.get("popularity")
            ),
            reverse=True
        )

    return sorted(
        items,
        key=lambda item: popularity_number(
            item.get("popularity")
        ),
        reverse=True
    )


# ==================================================
# DISCOVERY FETCH PAGE
# ==================================================

async def fetch_discovery_page(
    client: httpx.AsyncClient,
    page: int,
    category: Optional[str],
    per_page: int = 50
):

    variables = {
        "page": page,
        "perPage": per_page,
        "sort": DISCOVER_SORTS.get(
            "popular",
            ["POPULARITY_DESC"]
        )
    }

    if category:

        category_value = category.upper()

        if category_value in (
            "FINISHED",
            "RELEASING",
            "HIATUS",
            "CANCELLED",
            "NOT_YET_RELEASED"
        ):

            variables["status"] = category_value

        elif category_value in (
            "MANGA",
            "NOVEL",
            "ONE_SHOT"
        ):

            variables["format"] = category_value

    response = await http_request(
        "POST",
        ANILIST_URL,
        json={
            "query": DISCOVER_QUERY,
            "variables": variables
        },
        timeout=REQUEST_TIMEOUT
    )

    try:

        data = response.json()

    finally:

        await response.aclose()

    if not isinstance(data, dict):
        return {}, []

    page_data = (
        data.get("data", {})
        .get("Page", {})
    )

    if not isinstance(page_data, dict):
        return {}, []

    page_info = page_data.get(
        "pageInfo",
        {}
    )

    items = page_data.get(
        "media",
        []
    )

    if not isinstance(page_info, dict):
        page_info = {}

    if not isinstance(items, list):
        items = []

    return page_info, [
        normalize_discovery_item(item)
        for item in items
        if isinstance(item, dict)
    ]


# ==================================================
# DISCOVERY PAGE LIMITS
# ==================================================

def discovery_page_limit(limit: int) -> int:

    limit = max(
        1,
        min(
            int(limit),
            MAX_RAW_ITEMS
        )
    )

    return min(
        20,
        math.ceil(
            MAX_RAW_ITEMS / 50
        )
    )


def discovery_max_reachable_page(
    limit: int
) -> int:

    limit = max(
        1,
        int(limit)
    )

    return max(
        1,
        math.ceil(
            MAX_RAW_ITEMS / limit
        )
    )


# ==================================================
# DISCOVERY CACHE KEY
# ==================================================

def discovery_cache_key(
    category: Optional[str],
    sort: str,
    page: int,
    limit: int
) -> str:

    return "|".join([
        safe_text(category).casefold(),
        safe_text(sort).casefold(),
        str(page),
        str(limit)
    ])


# ==================================================
# DISCOVERY VALIDATION
# ==================================================

def validate_discovery_params(
    page: int,
    limit: int
):

    if page < 1:
        raise HTTPException(
            status_code=400,
            detail="Page must be greater than zero."
        )

    if limit < 1:
        raise HTTPException(
            status_code=400,
            detail="Limit must be greater than zero."
        )

    if limit > 100:
        raise HTTPException(
            status_code=400,
            detail="Limit cannot exceed 100."
        )


# ==================================================
# DISCOVERY RAW COLLECTION
# ==================================================

def append_discovery_items(
    collected: list[dict],
    incoming: list[dict],
    max_items: int = MAX_RAW_ITEMS
):

    for item in incoming:

        if not isinstance(item, dict):
            continue

        if len(collected) >= max_items:
            break

        collected.append(item)

    return collected


# ==================================================
# DISCOVERY ERROR HANDLING
# ==================================================

def discovery_error_response(
    error: Exception
) -> dict:

    logger.exception(
        "Discovery request failed: %s",
        error
    )

    return {
        "success": False,
        "items": [],
        "error": "Unable to load discovery results."
    }


# ==================================================
# SEARCH CONFIGURATION
# ==================================================

def normalize_search_query(query: str) -> str:

    return safe_text(query).strip()


def search_cache_key(
    query: str,
    limit: int
) -> str:

    return "|".join([
        normalize_search_query(query).casefold(),
        str(limit)
    ])


# ==================================================
# ARABIC SEARCH VARIANTS
# ==================================================

SEARCH_MAX_VARIANTS = 5

ARABIC_TITLE_ALIASES = {}


def arabic_phonetic_query(
    query: str
) -> Optional[str]:

    query = safe_text(query)

    if not query:
        return None

    return None


def search_query_variants(
    query: str
) -> list[str]:

    query = normalize_search_query(query)

    if not query:
        return []

    variants = [query]

    normalized = normalize_name(query)

    known_alias = ARABIC_TITLE_ALIASES.get(
        normalized
    )

    if isinstance(known_alias, str):
        variants.append(known_alias)

    elif isinstance(known_alias, list):
        variants.extend(known_alias)

    # Add phonetic search for Arabic queries.
    if re.search(r"[\u0600-\u06ff]", query):

        phonetic = arabic_phonetic_query(
            query
        )

        if phonetic:
            variants.append(phonetic)

    return list(dict.fromkeys(
        value.strip()
        for value in variants
        if isinstance(value, str) and value.strip()
    ))


# ==================================================
# SOURCE SEARCH VARIANTS
# ==================================================

def _source_search_variants(
    query: str,
    source: str
) -> list[str]:

    variants = search_query_variants(
        query
    )

    normalized_query = normalize_name(
        query
    )

    aliases = ARABIC_TITLE_ALIASES.get(
        normalized_query,
        []
    )

    if isinstance(aliases, str):
        aliases = [aliases]

    if isinstance(aliases, list):
        variants.extend(aliases)

    if source.casefold() == "anilist":

        phonetic = arabic_phonetic_query(
            query
        )

        if phonetic:
            variants.append(phonetic)

    return list(dict.fromkeys(
        value.strip()
        for value in variants
        if isinstance(value, str) and value.strip()
    ))[:SEARCH_MAX_VARIANTS]
    # ==================================================
# SEARCH RESULT FILTERING
# ==================================================

def filter_search_results(
    results: list[dict],
    query: str
) -> list[dict]:

    variants = [
        normalize_name(value)
        for value in search_query_variants(query)
    ]

    variants = [
        value
        for value in variants
        if value
    ]

    if not variants:
        return results

    filtered = []

    for item in results:

        titles = [
            normalize_name(title)
            for title in title_values(item)
        ]

        titles = [
            title
            for title in titles
            if title
        ]

        if not titles:
            continue

        if any(
            variant in title or title in variant
            for variant in variants
            for title in titles
        ):

            filtered.append(item)

    return filtered


# ==================================================
# SEARCH SOURCE HELPER
# ==================================================

async def _search_one_source(
    client: httpx.AsyncClient,
    query: str,
    source: str
) -> list[dict]:

    source_functions = {
        "anilist": search_anilist,
        "mangadex": search_mangadex,
        "mangabaka": search_mangabaka
    }

    search_function = source_functions.get(
        source.casefold()
    )

    if search_function is None:
        return []

    variants = _source_search_variants(
        query,
        source
    )

    collected = []

    for variant in variants:

        try:

            results = await search_function(
                client,
                variant
            )

            if results:
                collected.extend(results)

        except Exception as exc:

            logger.warning(
                "%s search failed for query '%s': %s",
                source,
                variant,
                exc
            )

    return merge_search_results(
        collected
    )


# ==================================================
# SEARCH RESPONSE BUILDER
# ==================================================

def build_search_response(
    query: str,
    results: list[dict],
    limit: int
) -> dict:

    filtered = filter_search_results(
        results,
        query
    )

    sorted_results = sort_search_results(
        filtered,
        query
    )

    return {
        "success": True,
        "query": query,
        "total": len(sorted_results),
        "results": sorted_results[:limit],
        "version": APP_VERSION
    }


# ==================================================
# CATALOG SEARCH
# ==================================================

@app.get("/catalog/search")
async def catalog_search(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100)
):

    query = normalize_search_query(q)

    if not query:
        raise HTTPException(
            status_code=400,
            detail="Search query cannot be empty."
        )

    cache_key = search_cache_key(
        query,
        limit
    )

    cached = await search_cache.get(
        cache_key
    )

    if cached is not None:
        return cached

    client = get_client()

    sources = [
        "anilist",
        "mangadex",
        "mangabaka"
    ]

    tasks = [
        _search_one_source(
            client,
            query,
            source
        )
        for source in sources
    ]

    source_results = await asyncio.gather(
        *tasks,
        return_exceptions=True
    )

    collected = []

    for result in source_results:

        if isinstance(result, Exception):

            logger.warning(
                "Search source failed: %s",
                result
            )

            continue

        if isinstance(result, list):
            collected.extend(result)

    merged = merge_search_results(
        collected
    )

    response = build_search_response(
        query,
        merged,
        limit
    )

    await search_cache.set(
        cache_key,
        response
    )

    return response


# ==================================================
# CATALOG DISCOVER
# ==================================================

@app.get("/catalog/discover")
async def catalog_discover(
    category: Optional[str] = None,
    sort: str = "popular",
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    include_adult: bool = False
):

    validate_discovery_params(
        page,
        limit
    )

    category = (
        category.strip()
        if category
        else None
    )

    sort = safe_text(sort).casefold()

    if sort not in DISCOVER_SORTS:
        sort = "popular"

    cache_key = discovery_cache_key(
        category,
        sort,
        page,
        limit
    )

    cached = await discovery_cache.get(
        cache_key
    )

    if cached is not None:
        return cached

    client = get_client()

    start_index = (
        page - 1
    ) * limit

    end_index = (
        start_index + limit
    )

    scan_limit = discovery_page_limit(
        limit
    )

    collected_raw = []
    collected_filtered = []

    upstream_has_next = True
    pages_scanned = 0

    try:

        for current_page in range(
            1,
            scan_limit + 1
        ):

            if not upstream_has_next:
                break

            page_info, page_items = (
                await fetch_discovery_page(
                    client,
                    current_page,
                    category,
                    50
                )
            )

            pages_scanned += 1

            upstream_has_next = bool(
                page_info.get("hasNextPage", False)
            )

            append_discovery_items(
                collected_raw,
                page_items,
                MAX_RAW_ITEMS
            )

            filtered_page = filter_discovery_items(
                page_items,
                category,
                include_adult
            )

            collected_filtered.extend(
                filtered_page
            )

            # Keep one extra item to determine
            # whether another result exists.
            if len(collected_filtered) > end_index:
                break

            if len(collected_raw) >= MAX_RAW_ITEMS:
                break

        sorted_items = sort_discovery_items(
            collected_filtered,
            sort
        )

        total_collected = len(
            sorted_items
        )

        results = sorted_items[
            start_index:end_index
        ]

        has_next_page = (
            total_collected > end_index
        )

        # If the upstream API has more pages
        # and the raw collection limit was reached,
        # preserve the possibility of another page.
        if (
            len(collected_raw) >= MAX_RAW_ITEMS
            and upstream_has_next
        ):

            has_next_page = True

        response = {
            "success": True,
            "category": category,
            "sort": sort,
            "page": page,
            "limit": limit,
            "total": total_collected,
            "has_next_page": has_next_page,
            "pages_scanned": pages_scanned,
            "results": results,
            "version": APP_VERSION
        }

        await discovery_cache.set(
            cache_key,
            response
        )

        return response

    except HTTPException:
        raise

    except Exception as exc:

        logger.exception(
            "Catalog discovery failed: %s",
            exc
        )

        raise HTTPException(
            status_code=502,
            detail="Unable to load discovery results."
        )
