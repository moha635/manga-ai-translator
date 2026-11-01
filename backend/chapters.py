"""
Manga AI Translator — Chapters module (corrected version).

- يحافظ على router والمسارات القديمة.
- MangaDex هو مصدر الفصول الحالي.
- AniList يُستخدم للمعلومات ومطابقة هوية العمل.
- Pagination تلقائي مع كشف الدوران.
- Cache وProgress في الذاكرة فقط.
- لا يحتاج مكتبات إضافية أو قاعدة بيانات.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import sys
import time
import unicodedata
import uuid

from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from difflib import SequenceMatcher
from typing import Any, Iterable, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query


# ==================================================
# ROUTER
# ==================================================

router = APIRouter(prefix="/api/v1", tags=["Chapters"])


# ==================================================
# CONSTANTS
# ==================================================

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

MAX_LANGUAGES_PER_REQUEST = 200
REQUEST_TIMEOUT = 25.0

HTTP_RETRY_ATTEMPTS = 2
HTTP_RETRY_BACKOFF = 0.6
MAX_RETRY_AFTER = 30.0

RETRYABLE_STATUS_CODES = {
    429, 500, 502, 503, 504
}

MAX_FEED_PAGES = 200
FEED_PAGE_SIZE = 500

FEED_CACHE_TTL = 300
FEED_CACHE_MAX_SIZE = 300

LANGUAGES_CACHE_TTL = 900
LANGUAGES_CACHE_MAX_SIZE = 200

RESOLVE_CACHE_TTL = 600
RESOLVE_CACHE_MAX_SIZE = 500

PROGRESS_TTL = 600
PROGRESS_MAX_SIZE = 200

TITLE_MATCH_THRESHOLD = 0.90
TITLE_MATCH_MIN_MARGIN = 0.06

logger = logging.getLogger(__name__)


# ==================================================
# SHARED HTTP CLIENT
# ==================================================

def _resolve_shared_client() -> Optional[httpx.AsyncClient]:
    """
    يستعمل HTTP client الموجود في main.py إن كان جاهزاً،
    دون استيراد main.py وتكوين circular import.
    """

    module = sys.modules.get("main")

    getter = getattr(module, "get_client", None) if module else None

    if not callable(getter):
        return None

    try:
        client = getter()
    except Exception:
        return None

    if client is None or getattr(client, "is_closed", True):
        return None

    return client


@asynccontextmanager
async def _client_context():
    """
    استعمال client المشترك إن وُجد،
    وإلا إنشاء client محلي مؤقت.
    """

    shared = _resolve_shared_client()

    if shared is not None:
        yield shared
        return

    async with httpx.AsyncClient(
        headers=HEADERS,
        timeout=httpx.Timeout(REQUEST_TIMEOUT),
        follow_redirects=True,
    ) as local:
        yield local


# ==================================================
# RETRY / HTTP ERRORS
# ==================================================

class HttpFailure(Exception):

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        retryable: bool = False,
    ):
        super().__init__(message)

        self.status_code = status_code
        self.retryable = retryable


def parse_retry_after(
    value: Optional[str],
) -> Optional[float]:

    if not value:
        return None

    value = value.strip()

    try:
        seconds = float(value)

        if math.isfinite(seconds):
            return max(0.0, seconds)

    except (ValueError, TypeError):
        pass

    try:
        retry_date = parsedate_to_datetime(value)

        if retry_date.tzinfo is None:
            retry_date = retry_date.replace(
                tzinfo=timezone.utc
            )

        seconds = (
            retry_date - datetime.now(timezone.utc)
        ).total_seconds()

        return max(0.0, seconds)

    except (
        TypeError,
        ValueError,
        OverflowError,
    ):
        return None


async def _sleep_backoff(
    attempt: int,
    retry_after: Optional[float] = None,
) -> None:

    if retry_after is not None:
        delay = min(retry_after, MAX_RETRY_AFTER)
    else:
        delay = min(
            HTTP_RETRY_BACKOFF * (2 ** attempt),
            MAX_RETRY_AFTER,
        )

    if delay > 0:
        await asyncio.sleep(delay)


async def _http_get_json(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[dict] = None,
) -> tuple[int, dict]:

    last_error = None

    for attempt in range(HTTP_RETRY_ATTEMPTS + 1):

        response = None

        try:
            response = await client.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            status = response.status_code

            # Retry للحالات المؤقتة
            if status in RETRYABLE_STATUS_CODES:

                retry_after = parse_retry_after(
                    response.headers.get("Retry-After")
                )

                if attempt < HTTP_RETRY_ATTEMPTS:

                    await response.aclose()
                    response = None

                    await _sleep_backoff(
                        attempt,
                        retry_after,
                    )

                    continue

                raise HttpFailure(
                    f"HTTP {status} from {url}",
                    status_code=status,
                    retryable=True,
                )

            # أخطاء HTTP غير القابلة للإعادة
            if status >= 400:

                raise HttpFailure(
                    f"HTTP {status} from {url}",
                    status_code=status,
                    retryable=False,
                )

            # قراءة JSON
            try:
                payload = response.json()

            except ValueError as exc:

                last_error = exc

                if attempt < HTTP_RETRY_ATTEMPTS:

                    await response.aclose()
                    response = None

                    await _sleep_backoff(attempt)

                    continue

                raise HttpFailure(
                    f"Invalid JSON from {url}",
                    status_code=502,
                ) from exc

            if not isinstance(payload, dict):

                raise HttpFailure(
                    f"Unexpected JSON payload from {url}",
                    status_code=502,
                )

            return status, payload

        except HttpFailure:
            raise

        except httpx.TimeoutException as exc:

            last_error = exc

            if attempt >= HTTP_RETRY_ATTEMPTS:

                raise HttpFailure(
                    f"Timeout while calling {url}",
                    status_code=504,
                    retryable=True,
                ) from exc

            await _sleep_backoff(attempt)

        except httpx.RequestError as exc:

            last_error = exc

            if attempt >= HTTP_RETRY_ATTEMPTS:

                raise HttpFailure(
                    f"Connection error for {url}: {exc}",
                    status_code=502,
                    retryable=True,
                ) from exc

            await _sleep_backoff(attempt)

        finally:

            if response is not None:
                await response.aclose()

    raise HttpFailure(
        f"Request failed for {url}: {last_error}",
        status_code=502,
        retryable=True,
    )


def _http_exception(exc: HttpFailure) -> HTTPException:

    return HTTPException(
        status_code=exc.status_code or 502,
        detail=str(exc),
    )


# ==================================================
# HTTP POST JSON
# ==================================================

async def _http_post_json(
    client: httpx.AsyncClient,
    url: str,
    json_body: dict,
) -> tuple[int, dict]:

    last_error = None

    for attempt in range(HTTP_RETRY_ATTEMPTS + 1):

        response = None

        try:
            response = await client.post(
                url,
                json=json_body,
                timeout=REQUEST_TIMEOUT,
            )

            status = response.status_code

            if status in RETRYABLE_STATUS_CODES:

                retry_after = parse_retry_after(
                    response.headers.get("Retry-After")
                )

                if attempt < HTTP_RETRY_ATTEMPTS:

                    await response.aclose()
                    response = None

                    await _sleep_backoff(
                        attempt,
                        retry_after,
                    )

                    continue

                raise HttpFailure(
                    f"HTTP {status} from {url}",
                    status_code=status,
                    retryable=True,
                )

            if status >= 400:

                raise HttpFailure(
                    f"HTTP {status} from {url}",
                    status_code=status,
                )

            try:
                payload = response.json()

            except ValueError as exc:

                last_error = exc

                if attempt < HTTP_RETRY_ATTEMPTS:

                    await response.aclose()
                    response = None

                    await _sleep_backoff(attempt)

                    continue

                raise HttpFailure(
                    f"Invalid JSON from {url}",
                    status_code=502,
                ) from exc

            if not isinstance(payload, dict):

                raise HttpFailure(
                    f"Unexpected JSON payload from {url}",
                    status_code=502,
                )

            return status, payload

        except HttpFailure:
            raise

        except httpx.TimeoutException as exc:

            last_error = exc

            if attempt >= HTTP_RETRY_ATTEMPTS:

                raise HttpFailure(
                    f"Timeout while calling {url}",
                    status_code=504,
                    retryable=True,
                ) from exc

            await _sleep_backoff(attempt)

        except httpx.RequestError as exc:

            last_error = exc

            if attempt >= HTTP_RETRY_ATTEMPTS:

                raise HttpFailure(
                    f"Connection error for {url}: {exc}",
                    status_code=502,
                    retryable=True,
                ) from exc

            await _sleep_backoff(attempt)

        finally:

            if response is not None:
                await response.aclose()

    raise HttpFailure(
        f"Request failed for {url}: {last_error}",
        status_code=502,
        retryable=True,
    )
    # ==================================================
# TTL CACHE
# ==================================================

class _TTLCache:

    def __init__(
        self,
        ttl: int,
        max_size: int = 200,
    ):
        self.ttl = ttl
        self.max_size = max_size
        self.data = OrderedDict()
        self.lock = asyncio.Lock()

    async def get(self, key: str):

        now = time.monotonic()

        async with self.lock:

            item = self.data.get(key)

            if item is None:
                return None

            expires_at, value = item

            if expires_at <= now:
                self.data.pop(key, None)
                return None

            self.data.move_to_end(key)

            return value

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ):

        duration = self.ttl if ttl is None else ttl

        async with self.lock:

            self.data[key] = (
                time.monotonic() + duration,
                value,
            )

            self.data.move_to_end(key)

            while len(self.data) > self.max_size:
                self.data.popitem(last=False)

    async def delete(self, key: str):

        async with self.lock:
            self.data.pop(key, None)

    async def clear(self):

        async with self.lock:
            self.data.clear()


# ==================================================
# CACHES
# ==================================================

_feed_cache = _TTLCache(
    FEED_CACHE_TTL,
    FEED_CACHE_MAX_SIZE,
)

_languages_cache = _TTLCache(
    LANGUAGES_CACHE_TTL,
    LANGUAGES_CACHE_MAX_SIZE,
)

_resolve_cache = _TTLCache(
    RESOLVE_CACHE_TTL,
    RESOLVE_CACHE_MAX_SIZE,
)


# ==================================================
# PROGRESS STORE
# ==================================================

class _ProgressStore:
    """
    حفظ تقدم جلب الفصول في الذاكرة فقط.

    ملاحظة:
    البيانات تضيع عند إعادة تشغيل التطبيق أو Render.
    """

    def __init__(
        self,
        ttl: int = PROGRESS_TTL,
        max_size: int = PROGRESS_MAX_SIZE,
    ):
        self.ttl = ttl
        self.max_size = max_size
        self.data = OrderedDict()
        self.lock = asyncio.Lock()

    def _cleanup_locked(self):

        now = time.monotonic()

        expired = [
            key
            for key, item in self.data.items()
            if item["expires_at"] <= now
        ]

        for key in expired:
            self.data.pop(key, None)

    async def update(
        self,
        key: str,
        **values,
    ) -> dict:

        async with self.lock:

            self._cleanup_locked()

            current = self.data.get(key, {})

            record = {
                **current,
                **values,
                "updated_at": time.time(),
                "expires_at": time.monotonic() + self.ttl,
            }

            self.data[key] = record
            self.data.move_to_end(key)

            while len(self.data) > self.max_size:
                self.data.popitem(last=False)

            return dict(record)

    async def snapshot(
        self,
        key: str,
    ) -> Optional[dict]:

        async with self.lock:

            self._cleanup_locked()

            item = self.data.get(key)

            if item is None:
                return None

            self.data.move_to_end(key)

            result = dict(item)

            result.pop("expires_at", None)

            return result

    async def delete(self, key: str):

        async with self.lock:
            self.data.pop(key, None)


_progress_store = _ProgressStore()


# ==================================================
# GENERAL HELPERS
# ==================================================

def _safe_int(
    value: Any,
    default: Optional[int] = None,
) -> Optional[int]:

    if value is None:
        return default

    try:
        return int(value)

    except (ValueError, TypeError, OverflowError):
        return default


def _clean_text(value: Any) -> str:

    if value is None:
        return ""

    text = str(value)

    text = unicodedata.normalize(
        "NFKC",
        text,
    )

    text = text.replace("\u200b", "")
    text = text.replace("\ufeff", "")

    text = re.sub(r"\s+", " ", text)

    return text.strip()


def _normalize_title(value: Any) -> str:

    text = _clean_text(value).casefold()

    text = unicodedata.normalize(
        "NFKD",
        text,
    )

    text = "".join(
        char
        for char in text
        if not unicodedata.combining(char)
    )

    text = text.replace("&", " and ")

    text = re.sub(
        r"[^a-z0-9\u0600-\u06ff]+",
        " ",
        text,
    )

    return re.sub(r"\s+", " ", text).strip()


def normalize_title(value: Any) -> str:
    return _normalize_title(value)


# ==================================================
# TITLE SIMILARITY
# ==================================================

def title_similarity(
    first: Any,
    second: Any,
) -> float:

    a = _normalize_title(first)
    b = _normalize_title(second)

    if not a or not b:
        return 0.0

    if a == b:
        return 1.0

    sequence_score = SequenceMatcher(
        None,
        a,
        b,
    ).ratio()

    words_a = set(a.split())
    words_b = set(b.split())

    if not words_a or not words_b:
        return sequence_score

    intersection = len(words_a & words_b)

    union = len(words_a | words_b)

    jaccard_score = (
        intersection / union
        if union
        else 0.0
    )

    containment_score = (
        intersection / min(
            len(words_a),
            len(words_b),
        )
    )

    return max(
        sequence_score,
        jaccard_score,
        containment_score * 0.85,
    )


# ==================================================
# TITLE EXTRACTION
# ==================================================

def extract_titles(
    manga: dict,
) -> list[str]:

    titles = []

    if not isinstance(manga, dict):
        return titles

    attributes = manga.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        attributes = {}

    title_data = attributes.get(
        "title",
        {},
    )

    alt_titles = attributes.get(
        "altTitles",
        [],
    )

    if isinstance(title_data, dict):

        for value in title_data.values():

            title = _clean_text(value)

            if title:
                titles.append(title)

    if isinstance(alt_titles, list):

        for item in alt_titles:

            if not isinstance(item, dict):
                continue

            for value in item.values():

                title = _clean_text(value)

                if title:
                    titles.append(title)

    # إزالة التكرار مع الحفاظ على الترتيب
    result = []
    seen = set()

    for title in titles:

        normalized = _normalize_title(title)

        if not normalized or normalized in seen:
            continue

        seen.add(normalized)
        result.append(title)

    return result


def _best_title_similarity(
    titles_a: Iterable[str],
    titles_b: Iterable[str],
) -> float:

    best = 0.0

    for title_a in titles_a:

        for title_b in titles_b:

            score = title_similarity(
                title_a,
                title_b,
            )

            if score > best:
                best = score

            if best >= 1.0:
                return 1.0

    return best


# ==================================================
# MANGADEX TITLE HELPERS
# ==================================================

def _mangadex_title(
    manga: dict,
) -> str:

    titles = extract_titles(manga)

    if not titles:
        return "Unknown"

    attributes = manga.get(
        "attributes",
        {},
    )

    primary = attributes.get(
        "title",
        {},
    )

    if isinstance(primary, dict):

        for language in (
            "en",
            "ja-ro",
            "ja",
        ):

            value = _clean_text(
                primary.get(language)
            )

            if value:
                return value

    return titles[0]


def _mangadex_description(
    manga: dict,
) -> str:

    attributes = manga.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        return ""

    descriptions = attributes.get(
        "description",
        {},
    )

    if not isinstance(descriptions, dict):
        return ""

    for language in (
        "en",
        "ar",
        "fr",
    ):

        value = _clean_text(
            descriptions.get(language)
        )

        if value:
            return value

    for value in descriptions.values():

        value = _clean_text(value)

        if value:
            return value

    return ""


def _mangadex_cover(
    manga: dict,
) -> Optional[str]:

    relationships = manga.get(
        "relationships",
        [],
    )

    if not isinstance(relationships, list):
        return None

    for relation in relationships:

        if not isinstance(relation, dict):
            continue

        if relation.get("type") != "cover_art":
            continue

        cover_id = relation.get("id")

        if not cover_id:
            continue

        attributes = relation.get(
            "attributes",
            {},
        )

        filename = (
            attributes.get("fileName")
            if isinstance(attributes, dict)
            else None
        )

        if filename:
            return (
                "https://uploads.mangadex.org/"
                f"covers/{manga.get('id')}/{filename}"
            )

    return None


def _mangadex_status(
    manga: dict,
) -> Optional[str]:

    attributes = manga.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        return None

    status = attributes.get("status")

    return (
        str(status).strip().lower()
        if status
        else None
    )


def _mangadex_year(
    manga: dict,
) -> Optional[int]:

    attributes = manga.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        return None

    return _safe_int(
        attributes.get("year")
    )


def _mangadex_original_language(
    manga: dict,
) -> Optional[str]:

    attributes = manga.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        return None

    language = attributes.get(
        "originalLanguage"
    )

    return (
        str(language).strip().lower()
        if language
        else None
    )


# ==================================================
# MANGADEX REQUEST HELPERS
# ==================================================

async def _mangadex_get(
    client: httpx.AsyncClient,
    endpoint: str,
    params: Optional[dict] = None,
) -> dict:

    url = (
        endpoint
        if endpoint.startswith("http")
        else f"{MANGADEX_URL}{endpoint}"
    )

    _, payload = await _http_get_json(
        client,
        url,
        params=params,
    )

    return payload


async def _anilist_post(
    client: httpx.AsyncClient,
    query: str,
    variables: Optional[dict] = None,
) -> dict:

    body = {
        "query": query,
        "variables": variables or {},
    }

    _, payload = await _http_post_json(
        client,
        ANILIST_URL,
        body,
    )

    errors = payload.get("errors")

    if errors:
        raise HttpFailure(
            "AniList GraphQL returned errors",
            status_code=502,
        )

    return payload.get("data", {})


# ==================================================
# LANGUAGE HELPERS
# ==================================================

def _language_name(code: str) -> str:

    code = str(code or "").lower().strip()

    return LANGUAGES.get(
        code,
        code,
    )


def _valid_language_code(
    code: Any,
) -> Optional[str]:

    if not isinstance(code, str):
        return None

    code = code.strip().lower()

    if not code:
        return None

    if not re.fullmatch(
        r"[a-z]{2,3}(?:-[a-z0-9]{2,8})?",
        code,
    ):
        return None

    return code


def _language_list(
    values: Iterable[Any],
) -> list[str]:

    result = []
    seen = set()

    for value in values:

        code = _valid_language_code(value)

        if not code or code in seen:
            continue

        seen.add(code)
        result.append(code)

    return result


# ==================================================
# MANGADEX FEED RESPONSE HELPERS
# ==================================================

def _extract_feed_data(
    payload: dict,
) -> list[dict]:

    data = payload.get("data", [])

    if not isinstance(data, list):
        return []

    return [
        item
        for item in data
        if isinstance(item, dict)
    ]


def _extract_feed_total(
    payload: dict,
) -> Optional[int]:

    total = payload.get("total")

    return _safe_int(total)


def _chapter_language(
    chapter: dict,
) -> Optional[str]:

    attributes = chapter.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        return None

    return _valid_language_code(
        attributes.get("translatedLanguage")
    )


def _chapter_number(
    chapter: dict,
) -> Optional[float]:

    attributes = chapter.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        return None

    value = attributes.get("chapter")

    if value is None:
        return None

    try:
        number = float(str(value).strip())

        if math.isfinite(number):
            return number

    except (ValueError, TypeError, OverflowError):
        pass

    return None


def _chapter_volume(
    chapter: dict,
) -> Optional[str]:

    attributes = chapter.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        return None

    value = attributes.get("volume")

    if value is None:
        return None

    return str(value).strip() or None


def _chapter_title(
    chapter: dict,
) -> str:

    attributes = chapter.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        return ""

    return _clean_text(
        attributes.get("title")
    )


def _chapter_pages(
    chapter: dict,
) -> Optional[int]:

    attributes = chapter.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        return None

    return _safe_int(
        attributes.get("pages")
    )


def _chapter_published_at(
    chapter: dict,
) -> Optional[str]:

    attributes = chapter.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        return None

    return (
        attributes.get("publishAt")
        or attributes.get("createdAt")
    )


def _chapter_external_url(
    chapter: dict,
) -> Optional[str]:

    attributes = chapter.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        return None

    return attributes.get("externalUrl")


# ==================================================
# CHAPTER IDENTIFICATION
# ==================================================

def _chapter_identity(
    chapter: dict,
) -> str:

    chapter_id = chapter.get("id")

    if chapter_id:
        return f"id:{chapter_id}"

    attributes = chapter.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        attributes = {}

    number = attributes.get("chapter")
    volume = attributes.get("volume")
    title = _clean_text(
        attributes.get("title")
    )

    language = attributes.get(
        "translatedLanguage"
    )

    groups = []

    relationships = chapter.get(
        "relationships",
        [],
    )

    if isinstance(relationships, list):

        for relation in relationships:

            if (
                isinstance(relation, dict)
                and relation.get("type") == "scanlation_group"
            ):
                groups.append(
                    relation.get("id", "")
                )

    # المفتاح البديل لا يدمج الفصول
    # التي لها نفس الرقم لكن معرفات مختلفة.
    return "|".join([
        str(number or ""),
        str(volume or ""),
        title,
        str(language or ""),
        ",".join(sorted(groups)),
    ])
    # ==================================================
# FETCH AVAILABLE LANGUAGES
# ==================================================

async def fetch_available_languages(
    manga_id: str,
    client: Optional[httpx.AsyncClient] = None,
) -> list[str]:
    """
    اكتشاف اللغات الموجودة فعلياً في فصول العمل.
    لا يعتمد على /manga/tag لأنه لا يمثل دليل اللغات.
    """

    manga_id = str(manga_id).strip()

    if not manga_id:
        return []

    cached = await _languages_cache.get(manga_id)

    if cached is not None:
        return list(cached)

    async def _fetch(active_client):

        languages = set()
        offset = 0
        visited_offsets = set()

        for _ in range(MAX_FEED_PAGES):

            if offset in visited_offsets:
                break

            visited_offsets.add(offset)

            params = {
                "manga": [manga_id],
                "limit": FEED_PAGE_SIZE,
                "offset": offset,
                "order[chapter]": "asc",
                "includes[]": ["scanlation_group"],
            }

            payload = await _mangadex_get(
                active_client,
                "/chapter",
                params,
            )

            chapters = _extract_feed_data(payload)

            for chapter in chapters:

                language = _chapter_language(chapter)

                if language:
                    languages.add(language)

            total = _extract_feed_total(payload)

            if not chapters:
                break

            offset += len(chapters)

            if total is not None and offset >= total:
                break

            if len(chapters) < FEED_PAGE_SIZE:
                break

        return sorted(languages)

    if client is not None:
        result = await _fetch(client)
    else:
        async with _client_context() as active_client:
            result = await _fetch(active_client)

    await _languages_cache.set(
        manga_id,
        result,
    )

    return result


# ==================================================
# CHAPTER SORTING
# ==================================================

def _chapter_sort_key(
    chapter: dict,
) -> tuple:

    attributes = chapter.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        attributes = {}

    raw_number = attributes.get("chapter")
    raw_volume = attributes.get("volume")

    number = _chapter_number(chapter)

    volume = _safe_int(raw_volume, -1)

    if volume is None:
        volume = -1

    if number is not None:

        return (
            0,
            volume,
            number,
            _chapter_title(chapter).casefold(),
            str(chapter.get("id", "")),
        )

    # الفصول الخاصة أو التي لا تحمل رقماً
    title = _chapter_title(chapter).casefold()

    return (
        1,
        volume,
        title,
        str(raw_number or "").casefold(),
        str(chapter.get("id", "")),
    )


def chapter_sort_key(chapter: dict) -> tuple:
    return _chapter_sort_key(chapter)


# ==================================================
# CHAPTER CLASSIFICATION
# ==================================================

def _chapter_type(
    chapter: dict,
) -> str:

    number = _chapter_number(chapter)

    if number is not None:
        return "numbered"

    title = _chapter_title(chapter).casefold()

    if not title:
        return "special"

    special_words = (
        "extra",
        "special",
        "side story",
        "oneshot",
        "one-shot",
        "omake",
        "bonus",
        "epilogue",
        "prologue",
        "afterword",
        "extras",
        "فصل خاص",
        "إضافي",
        "ملحق",
        "خاتمة",
        "مقدمة",
    )

    if any(word in title for word in special_words):
        return "special"

    return "special"


def classify_chapter(chapter: dict) -> str:
    return _chapter_type(chapter)


# ==================================================
# CHAPTER SERIALIZATION
# ==================================================

def _serialize_chapter(
    chapter: dict,
) -> dict:

    attributes = chapter.get(
        "attributes",
        {},
    )

    if not isinstance(attributes, dict):
        attributes = {}

    chapter_id = chapter.get("id")

    number = attributes.get("chapter")
    volume = attributes.get("volume")

    language = _chapter_language(chapter)

    title = _chapter_title(chapter)

    pages = _chapter_pages(chapter)

    external_url = _chapter_external_url(chapter)

    published_at = _chapter_published_at(chapter)

    return {
        "id": chapter_id,
        "chapter_id": chapter_id,
        "chapter": number,
        "volume": volume,
        "title": title,
        "translatedLanguage": language,
        "language": language,
        "language_name": (
            _language_name(language)
            if language
            else None
        ),
        "pages": pages,
        "publishAt": published_at,
        "published_at": published_at,
        "externalUrl": external_url,
        "external_url": external_url,
        "type": _chapter_type(chapter),
        "url": (
            f"https://mangadex.org/chapter/{chapter_id}"
            if chapter_id
            else external_url
        ),
    }


# ==================================================
# CHAPTER DEDUPLICATION
# ==================================================

def _deduplicate_chapters(
    chapters: Iterable[dict],
) -> list[dict]:

    result = []
    seen = set()

    for chapter in chapters:

        if not isinstance(chapter, dict):
            continue

        identity = _chapter_identity(chapter)

        if identity in seen:
            continue

        seen.add(identity)

        result.append(chapter)

    return result


# ==================================================
# FETCH ALL CHAPTERS
# ==================================================

async def fetch_all_chapters(
    manga_id: str,
    language: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
    progress_key: Optional[str] = None,
) -> list[dict]:
    """
    جلب جميع صفحات الفصول تلقائياً.

    إذا فشلت صفحة في منتصف العملية، نرفع الخطأ
    ولا نرجع قائمة جزئية على أنها مكتملة.
    """

    manga_id = str(manga_id).strip()

    if not manga_id:
        raise HttpFailure(
            "Missing MangaDex manga ID",
            status_code=400,
        )

    language = (
        _valid_language_code(language)
        if language
        else None
    )

    cache_key = (
        f"{manga_id}:{language or 'all'}"
    )

    cached = await _feed_cache.get(cache_key)

    if cached is not None:
        return [dict(item) for item in cached]

    async def _fetch(active_client):

        all_chapters = []

        offset = 0
        visited_offsets = set()

        total = None

        for page_index in range(MAX_FEED_PAGES):

            if offset in visited_offsets:

                raise HttpFailure(
                    "MangaDex pagination loop detected",
                    status_code=502,
                )

            visited_offsets.add(offset)

            params = {
                "manga": [manga_id],
                "limit": FEED_PAGE_SIZE,
                "offset": offset,
                "order[chapter]": "asc",
                "includes[]": ["scanlation_group"],
            }

            if language:
                params["translatedLanguage[]"] = [language]

            if progress_key:

                await _progress_store.update(
                    progress_key,
                    manga_id=manga_id,
                    language=language,
                    status="running",
                    page=page_index + 1,
                    offset=offset,
                    chapters_fetched=len(all_chapters),
                    total=total,
                )

            payload = await _mangadex_get(
                active_client,
                "/chapter",
                params,
            )

            page_chapters = _extract_feed_data(payload)

            page_total = _extract_feed_total(payload)

            if page_total is not None:
                total = page_total

            if not page_chapters:
                break

            all_chapters.extend(page_chapters)

            previous_offset = offset

            offset += len(page_chapters)

            if offset <= previous_offset:

                raise HttpFailure(
                    "MangaDex pagination did not advance",
                    status_code=502,
                )

            if total is not None and offset >= total:
                break

            if len(page_chapters) < FEED_PAGE_SIZE:
                break

        else:

            raise HttpFailure(
                "Maximum MangaDex pagination limit reached",
                status_code=502,
            )

        # إذا أعاد المصدر total أكبر من عدد النتائج،
        # لا نعتبر القائمة مكتملة.
        if total is not None and len(all_chapters) < total:

            raise HttpFailure(
                (
                    "Incomplete MangaDex feed: "
                    f"received {len(all_chapters)} of {total}"
                ),
                status_code=502,
            )

        all_chapters = _deduplicate_chapters(
            all_chapters
        )

        all_chapters.sort(
            key=_chapter_sort_key
        )

        if progress_key:

            await _progress_store.update(
                progress_key,
                manga_id=manga_id,
                language=language,
                status="completed",
                page=page_index + 1,
                offset=offset,
                chapters_fetched=len(all_chapters),
                total=total,
            )

        return all_chapters

    try:

        if client is not None:
            result = await _fetch(client)

        else:
            async with _client_context() as active_client:
                result = await _fetch(active_client)

    except HttpFailure as exc:

        if progress_key:

            await _progress_store.update(
                progress_key,
                status="failed",
                error=str(exc),
                error_status=exc.status_code,
            )

        raise

    except Exception as exc:

        if progress_key:

            await _progress_store.update(
                progress_key,
                status="failed",
                error=str(exc),
            )

        raise HttpFailure(
            f"Failed to fetch chapters: {exc}",
            status_code=502,
        ) from exc

    # لا نحفظ إلا النتائج التي اكتمل جلبها
    await _feed_cache.set(
        cache_key,
        result,
    )

    return [dict(item) for item in result]


# ==================================================
# GROUP CHAPTERS BY LANGUAGE
# ==================================================

def _group_chapters_by_language(
    chapters: Iterable[dict],
) -> dict[str, list[dict]]:

    groups = {}

    for chapter in chapters:

        language = _chapter_language(chapter)

        if not language:
            continue

        groups.setdefault(
            language,
            [],
        ).append(chapter)

    for language in groups:

        groups[language].sort(
            key=_chapter_sort_key
        )

    return groups


def group_chapters_by_language(
    chapters: Iterable[dict],
) -> dict[str, list[dict]]:

    return _group_chapters_by_language(chapters)


# ==================================================
# CHAPTER ALTERNATIVES
# ==================================================

def _chapter_alternatives(
    chapters: Iterable[dict],
) -> dict[str, list[dict]]:
    """
    تجميع نسخ الفصل نفسه حسب الرقم واللغة.

    لا نحذف الفصول ذات المعرفات المختلفة.
    هذه الدالة تُستخدم للعرض فقط.
    """

    alternatives = {}

    for chapter in chapters:

        attributes = chapter.get(
            "attributes",
            {},
        )

        if not isinstance(attributes, dict):
            attributes = {}

        number = attributes.get("chapter")

        volume = attributes.get("volume")

        title = _chapter_title(chapter)

        language = _chapter_language(chapter)

        if number is not None:

            identity = (
                f"number:{volume or ''}:{number}"
            )

        elif title:

            identity = (
                f"special:{volume or ''}:"
                f"{_normalize_title(title)}"
            )

        else:

            identity = (
                f"unique:{chapter.get('id')}"
            )

        key = f"{language or 'unknown'}:{identity}"

        alternatives.setdefault(
            key,
            [],
        ).append(
            _serialize_chapter(chapter)
        )

    return alternatives


def get_chapter_alternatives(
    chapters: Iterable[dict],
) -> dict[str, list[dict]]:

    return _chapter_alternatives(chapters)


# ==================================================
# CHAPTER COUNTS
# ==================================================

def _chapter_counts(
    chapters: Iterable[dict],
) -> dict:

    total = 0
    numbered = 0
    special = 0
    languages = set()

    for chapter in chapters:

        total += 1

        if _chapter_type(chapter) == "numbered":
            numbered += 1
        else:
            special += 1

        language = _chapter_language(chapter)

        if language:
            languages.add(language)

    return {
        "total": total,
        "numbered": numbered,
        "special": special,
        "languages": sorted(languages),
        "language_count": len(languages),
    }


# ==================================================
# RESPONSE BUILDERS
# ==================================================

def _build_chapter_response(
    manga_id: str,
    chapters: list[dict],
    language: Optional[str] = None,
) -> dict:

    serialized = [
        _serialize_chapter(chapter)
        for chapter in chapters
    ]

    grouped = {}

    for chapter in serialized:

        code = chapter.get("language")

        if code:
            grouped.setdefault(code, []).append(chapter)

    for code in grouped:

        grouped[code].sort(
            key=lambda item: (
                _safe_int(item.get("volume"), -1) or -1,
                (
                    float(item["chapter"])
                    if item.get("chapter") is not None
                    and str(item["chapter"]).replace(".", "", 1).isdigit()
                    else float("inf")
                ),
                str(item.get("title") or "").casefold(),
            )
        )

    return {
        "manga_id": manga_id,
        "language": language,
        "total": len(serialized),
        "chapters": serialized,
        "languages": sorted(grouped.keys()),
        "chapters_by_language": grouped,
        "counts": _chapter_counts(chapters),
    }
    # ==================================================
# ANILIST
# ==================================================

ANILIST_SEARCH_QUERY = """
query ($search: String) {
    Media(search: $search, type: MANGA) {
        id
        idMal
        title {
            romaji
            english
            native
        }
        description
        coverImage {
            large
            medium
        }
        status
        startDate {
            year
        }
        chapters
        volumes
        countryOfOrigin
        format
        synonyms
    }
}
"""


async def fetch_anilist(
    title: str,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[dict]:

    title = _clean_text(title)

    if not title:
        return None

    cache_key = f"anilist:{_normalize_title(title)}"

    cached = await _resolve_cache.get(cache_key)

    if cached is not None:
        return dict(cached) if cached else None

    async def _fetch(active_client):

        data = await _anilist_post(
            active_client,
            ANILIST_SEARCH_QUERY,
            {"search": title},
        )

        media = data.get("Media")

        if not isinstance(media, dict):
            return None

        titles = media.get("title") or {}

        all_titles = []

        if isinstance(titles, dict):

            all_titles.extend([
                titles.get("romaji"),
                titles.get("english"),
                titles.get("native"),
            ])

        synonyms = media.get("synonyms") or []

        if isinstance(synonyms, list):
            all_titles.extend(synonyms)

        score = title_similarity(
            [title],
            [
                item
                for item in all_titles
                if item
            ],
        )

        # لا نقبل نتيجة بعيدة عن البحث
        if score < TITLE_MATCH_THRESHOLD:
            return None

        return media

    if client is not None:
        result = await _fetch(client)

    else:
        async with _client_context() as active_client:
            result = await _fetch(active_client)

    await _resolve_cache.set(
        cache_key,
        result,
    )

    return result


# ==================================================
# MANGADEX SEARCH
# ==================================================

async def search_mangadex(
    title: str,
    client: Optional[httpx.AsyncClient] = None,
    limit: int = 20,
) -> list[dict]:

    title = _clean_text(title)

    if not title:
        return []

    limit = max(1, min(int(limit), 100))

    cache_key = (
        f"search:{_normalize_title(title)}:{limit}"
    )

    cached = await _resolve_cache.get(cache_key)

    if cached is not None:
        return [dict(item) for item in cached]

    params = {
        "title": title,
        "limit": limit,
        "includes[]": ["cover_art", "author", "artist"],
        "order[relevance]": "desc",
    }

    async def _fetch(active_client):

        payload = await _mangadex_get(
            active_client,
            "/manga",
            params,
        )

        return _extract_feed_data(payload)

    if client is not None:
        result = await _fetch(client)

    else:
        async with _client_context() as active_client:
            result = await _fetch(active_client)

    await _resolve_cache.set(
        cache_key,
        result,
    )

    return [dict(item) for item in result]


# ==================================================
# MANGADEX MATCHING
# ==================================================

def match_mangadex(
    anilist: dict,
    candidates: list[dict],
) -> Optional[dict]:
    """
    اختيار عمل MangaDex بناءً على العناوين.

    لا نعتمد على تخمين MAL ID داخل relationships.
    إذا كانت النتائج متقاربة جداً نرفض الاختيار.
    """

    if not isinstance(anilist, dict):
        return None

    anilist_title_data = anilist.get(
        "title",
        {},
    )

    anilist_titles = []

    if isinstance(anilist_title_data, dict):

        anilist_titles.extend([
            anilist_title_data.get("romaji"),
            anilist_title_data.get("english"),
            anilist_title_data.get("native"),
        ])

    synonyms = anilist.get("synonyms") or []

    if isinstance(synonyms, list):
        anilist_titles.extend(synonyms)

    anilist_titles = [
        _clean_text(item)
        for item in anilist_titles
        if _clean_text(item)
    ]

    if not anilist_titles:
        return None

    ranked = []

    for manga in candidates:

        if not isinstance(manga, dict):
            continue

        manga_titles = extract_titles(manga)

        if not manga_titles:
            continue

        score = _best_title_similarity(
            anilist_titles,
            manga_titles,
        )

        attributes = manga.get(
            "attributes",
            {},
        )

        if not isinstance(attributes, dict):
            attributes = {}

        original_language = (
            attributes.get("originalLanguage")
        )

        country = anilist.get(
            "countryOfOrigin"
        )

        # إشارة مساعدة فقط، ليست دليلاً قاطعاً
        language_bonus = 0.0

        if country == "JP" and original_language == "ja":
            language_bonus = 0.01

        elif country == "KR" and original_language == "ko":
            language_bonus = 0.01

        elif country == "CN" and original_language == "zh":
            language_bonus = 0.01

        final_score = min(
            1.0,
            score + language_bonus,
        )

        ranked.append((
            final_score,
            manga,
        ))

    if not ranked:
        return None

    ranked.sort(
        key=lambda item: item[0],
        reverse=True,
    )

    best_score, best_manga = ranked[0]

    if best_score < TITLE_MATCH_THRESHOLD:
        return None

    if len(ranked) > 1:

        second_score = ranked[1][0]

        if (
            best_score - second_score
            < TITLE_MATCH_MIN_MARGIN
        ):
            return None

    return best_manga


# ==================================================
# RESOLVE MANGA
# ==================================================

async def resolve_manga(
    manga_id: str,
    client: Optional[httpx.AsyncClient] = None,
) -> dict:
    """
    استقبال AniList ID أو MangaDex ID.

    إذا كان UUID نحاول جلب MangaDex مباشرة.
    إذا كان رقماً نعتبره AniList ID ونبحث عن العمل.
    """

    manga_id = str(manga_id).strip()

    if not manga_id:
        raise HTTPException(
            status_code=400,
            detail="Missing manga ID",
        )

    cache_key = f"resolve:{manga_id}"

    cached = await _resolve_cache.get(cache_key)

    if cached is not None:
        return dict(cached)

    async def _fetch(active_client):

        # UUID: MangaDex ID
        if re.fullmatch(
            r"[0-9a-fA-F]{8}-"
            r"[0-9a-fA-F]{4}-"
            r"[0-9a-fA-F]{4}-"
            r"[0-9a-fA-F]{4}-"
            r"[0-9a-fA-F]{12}",
            manga_id,
        ):

            payload = await _mangadex_get(
                active_client,
                f"/manga/{manga_id}",
                {
                    "includes[]": [
                        "cover_art",
                        "author",
                        "artist",
                    ]
                },
            )

            manga = payload.get("data")

            if not isinstance(manga, dict):
                raise HTTPException(
                    status_code=404,
                    detail="MangaDex manga not found",
                )

            return {
                "mangadex_id": manga.get("id"),
                "anilist_id": None,
                "title": _mangadex_title(manga),
                "titles": extract_titles(manga),
                "description": _mangadex_description(manga),
                "cover": _mangadex_cover(manga),
                "status": _mangadex_status(manga),
                "year": _mangadex_year(manga),
                "source": "mangadex",
                "mangadex": manga,
                "anilist": None,
            }

        # رقم: AniList ID
        if not manga_id.isdigit():

            raise HTTPException(
                status_code=400,
                detail="Invalid manga ID",
            )

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
                description
                coverImage {
                    large
                    medium
                }
                status
                startDate {
                    year
                }
                chapters
                volumes
                countryOfOrigin
                format
                synonyms
            }
        }
        """

        data = await _anilist_post(
            active_client,
            query,
            {"id": int(manga_id)},
        )

        anilist = data.get("Media")

        if not isinstance(anilist, dict):

            raise HTTPException(
                status_code=404,
                detail="AniList manga not found",
            )

        title_data = anilist.get("title") or {}

        title = (
            title_data.get("english")
            or title_data.get("romaji")
            or title_data.get("native")
        )

        if not title:

            raise HTTPException(
                status_code=404,
                detail="Manga title not found",
            )

        candidates = await search_mangadex(
            title,
            client=active_client,
            limit=30,
        )

        matched = match_mangadex(
            anilist,
            candidates,
        )

        if not matched:

            raise HTTPException(
                status_code=404,
                detail="No reliable MangaDex match found",
            )

        return {
            "mangadex_id": matched.get("id"),
            "anilist_id": anilist.get("id"),
            "title": title,
            "titles": extract_titles(matched),
            "description": (
                anilist.get("description")
                or _mangadex_description(matched)
            ),
            "cover": (
                (anilist.get("coverImage") or {}).get("large")
                or _mangadex_cover(matched)
            ),
            "status": (
                anilist.get("status")
                or _mangadex_status(matched)
            ),
            "year": (
                (anilist.get("startDate") or {}).get("year")
                or _mangadex_year(matched)
            ),
            "source": "anilist+mangadex",
            "mangadex": matched,
            "anilist": anilist,
        }

    try:

        if client is not None:
            result = await _fetch(client)

        else:
            async with _client_context() as active_client:
                result = await _fetch(active_client)

    except HttpFailure as exc:
        raise _http_exception(exc) from exc

    await _resolve_cache.set(
        cache_key,
        result,
    )

    return result


# ==================================================
# FETCH LANGUAGE CHAPTERS
# ==================================================

async def fetch_language(
    manga_id: str,
    language: str,
    client: Optional[httpx.AsyncClient] = None,
) -> list[dict]:

    language = _valid_language_code(language)

    if not language:

        raise HTTPException(
            status_code=400,
            detail="Invalid language code",
        )

    try:

        return await fetch_all_chapters(
            manga_id,
            language=language,
            client=client,
        )

    except HttpFailure as exc:
        raise _http_exception(exc) from exc


# ==================================================
# MAIN CHAPTER ROUTE
# ==================================================

@router.get("/manga/{manga_id}/chapters")
async def get_chapters(
    manga_id: str,
    language: Optional[str] = Query(
        default=None,
        description="Optional translated language code",
    ),
):

    try:

        resolved = await resolve_manga(manga_id)

        actual_id = resolved.get("mangadex_id")

        if not actual_id:

            raise HTTPException(
                status_code=404,
                detail="MangaDex ID not found",
            )

        chapters = await fetch_all_chapters(
            actual_id,
            language=language,
        )

        response = _build_chapter_response(
            actual_id,
            chapters,
            language,
        )

        response["manga"] = {
            "title": resolved.get("title"),
            "cover": resolved.get("cover"),
            "status": resolved.get("status"),
            "year": resolved.get("year"),
            "source": resolved.get("source"),
        }

        return response

    except HTTPException:
        raise

    except HttpFailure as exc:
        raise _http_exception(exc) from exc


# ==================================================
# LEGACY CHAPTER-LIST ROUTE
# ==================================================

@router.get("/manga/{manga_id}/chapter-list")
async def get_chapter_list(
    manga_id: str,
    language: Optional[str] = None,
):

    try:

        resolved = await resolve_manga(manga_id)

        actual_id = resolved.get("mangadex_id")

        if not actual_id:

            raise HTTPException(
                status_code=404,
                detail="MangaDex ID not found",
            )

        chapters = await fetch_all_chapters(
            actual_id,
            language=language,
        )

        serialized = [
            _serialize_chapter(chapter)
            for chapter in chapters
        ]

        return {
            "manga_id": actual_id,
            "total": len(serialized),
            "chapters": serialized,
            "languages": _chapter_counts(chapters)["languages"],
        }

    except HTTPException:
        raise

    except HttpFailure as exc:
        raise _http_exception(exc) from exc


# ==================================================
# CHAPTER LIST PROGRESS
# ==================================================

@router.get("/manga/{manga_id}/chapter-list/progress")
async def get_chapter_list_progress(
    manga_id: str,
    language: Optional[str] = None,
):

    language = (
        _valid_language_code(language)
        if language
        else None
    )

    progress_key = (
        f"{manga_id}:{language or 'all'}"
    )

    progress = await _progress_store.snapshot(
        progress_key
    )

    if progress is None:

        return {
            "manga_id": manga_id,
            "language": language,
            "status": "not_started",
            "chapters_fetched": 0,
            "page": 0,
            "total": None,
        }

    return {
        "manga_id": manga_id,
        **progress,
    }


# ==================================================
# ALTERNATIVE LEGACY ROUTE
# ==================================================

@router.get("/manga/{manga_id}/chapter-list/languages")
async def get_chapter_list_languages(
    manga_id: str,
):

    try:

        resolved = await resolve_manga(manga_id)

        actual_id = resolved.get("mangadex_id")

        if not actual_id:

            raise HTTPException(
                status_code=404,
                detail="MangaDex ID not found",
            )

        languages = await fetch_available_languages(
            actual_id
        )

        return {
            "manga_id": actual_id,
            "languages": [
                {
                    "code": code,
                    "name": _language_name(code),
                }
                for code in languages
            ],
            "total": len(languages),
        }

    except HTTPException:
        raise

    except HttpFailure as exc:
        raise _http_exception(exc) from exc


# ==================================================
# LAZY SEMAPHORE
# ==================================================

_semaphore = None
_semaphore_loop = None


def get_semaphore(
    limit: int = 5,
) -> asyncio.Semaphore:

    global _semaphore
    global _semaphore_loop

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is None:
        return asyncio.Semaphore(limit)

    if (
        _semaphore is None
        or _semaphore_loop is not loop
    ):

        _semaphore = asyncio.Semaphore(limit)
        _semaphore_loop = loop

    return _semaphore


# ==================================================
# END OF CHAPTERS MODULE
# ==================================================
