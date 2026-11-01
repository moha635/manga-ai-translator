"""
Manga AI Translator — Chapters module
Corrected version 5.2 — MangaDex-Only Resolver

- يحافظ على router والمسارات القديمة.
- MangaDex هو المصدر الوحيد للمعرّف والفصول.
- البحث بالاسم مباشرة في MangaDex (بدون AniList).
- استخراج UUID تلقائياً من أفضل تطابق.
- جلب الفصول العربية افتراضياً إذا لم تُحدد لغة.
- خطأ 404 واضح عند غياب الفصول أو فشل المصدر.
- Pagination مع كشف الدوران والنتائج الناقصة.
- Cache وProgress في الذاكرة فقط.
"""

from __future__ import annotations

import asyncio
import logging
import math
import re
import sys
import time
import unicodedata

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

# AniList اختياري — غير مستخدم في resolve_manga
ANILIST_URL = "https://graphql.anilist.co"

HEADERS = {
    "User-Agent": "MangaAITranslator/5.2",
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
    429,
    500,
    502,
    503,
    504,
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

# حد أدنى لقبول تطابق البحث بالعنوان
TITLE_MATCH_THRESHOLD = 0.90
TITLE_MATCH_MIN_MARGIN = 0.06

# حد أخف للبحث المباشر بالاسم في MangaDex
MANGADEX_TITLE_MIN_SCORE = 0.50

# اللغة الافتراضية عند عدم تحديدها
DEFAULT_LANGUAGE = "ar"

logger = logging.getLogger(__name__)


# ==================================================
# SHARED HTTP CLIENT
# ==================================================

def _resolve_shared_client() -> Optional[httpx.AsyncClient]:
    """
    استعمال HTTP client الموجود في main.py إن كان جاهزاً،
    دون استيراد main.py وتكوين circular import.
    """

    module_names = (
        "main",
        "backend.main",
    )

    for module_name in module_names:

        module = sys.modules.get(module_name)

        if module is None:
            continue

        getter = getattr(
            module,
            "get_client",
            None,
        )

        if not callable(getter):
            continue

        try:
            client = getter()

        except Exception:
            logger.exception(
                "Could not retrieve shared HTTP client"
            )
            continue

        if (
            isinstance(client, httpx.AsyncClient)
            and not client.is_closed
        ):
            return client

    return None


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
# HTTP ERRORS
# ==================================================

class HttpFailure(Exception):
    """
    خطأ موحد لطلبات MangaDex (وAniList اختياريًا).
    """

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        retryable: bool = False,
    ):

        super().__init__(message)

        self.status_code = status_code
        self.retryable = retryable


def _http_exception(
    exc: HttpFailure,
) -> HTTPException:

    return HTTPException(
        status_code=exc.status_code or 502,
        detail=str(exc),
    )


# ==================================================
# RETRY HELPERS
# ==================================================

def parse_retry_after(
    value: Optional[str],
) -> Optional[float]:
    """
    قراءة Retry-After سواء كان عدداً بالثواني
    أو تاريخ HTTP.
    """

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

        delay = min(
            retry_after,
            MAX_RETRY_AFTER,
        )

    else:

        delay = min(
            HTTP_RETRY_BACKOFF * (2 ** attempt),
            MAX_RETRY_AFTER,
        )

    if delay > 0:
        await asyncio.sleep(delay)


# ==================================================
# HTTP RESPONSE HELPERS
# ==================================================

def _response_error_message(
    response: httpx.Response,
    url: str,
) -> str:
    """
    إظهار معلومات الخطأ دون تسجيل محتوى حساس.
    """

    status = response.status_code

    try:
        body = response.text[:500]

    except Exception:
        body = ""

    body = body.replace("\n", " ").strip()

    if body:
        return f"HTTP {status} from {url}: {body}"

    return f"HTTP {status} from {url}"


def _decode_json_response(
    response: httpx.Response,
    url: str,
) -> dict:

    try:
        payload = response.json()

    except ValueError as exc:

        raise HttpFailure(
            f"Invalid JSON response from {url}",
            status_code=502,
        ) from exc

    if not isinstance(payload, dict):

        raise HttpFailure(
            f"Unexpected JSON payload from {url}",
            status_code=502,
        )

    return payload


# ==================================================
# HTTP GET JSON
# ==================================================

async def _http_get_json(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[dict] = None,
) -> tuple[int, dict]:

    last_error = None

    for attempt in range(
        HTTP_RETRY_ATTEMPTS + 1
    ):

        response = None

        try:

            response = await client.get(
                url,
                params=params,
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
                    _response_error_message(
                        response,
                        url,
                    ),
                    status_code=status,
                    retryable=True,
                )

            if status >= 400:

                raise HttpFailure(
                    _response_error_message(
                        response,
                        url,
                    ),
                    status_code=status,
                    retryable=False,
                )

            payload = _decode_json_response(
                response,
                url,
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
# HTTP POST JSON (اختياري — لاستخدام AniList لاحقًا)
# ==================================================

async def _http_post_json(
    client: httpx.AsyncClient,
    url: str,
    json_body: dict,
) -> tuple[int, dict]:

    last_error = None

    for attempt in range(
        HTTP_RETRY_ATTEMPTS + 1
    ):

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
                    _response_error_message(
                        response,
                        url,
                    ),
                    status_code=status,
                    retryable=True,
                )

            if status >= 400:

                raise HttpFailure(
                    _response_error_message(
                        response,
                        url,
                    ),
                    status_code=status,
                    retryable=False,
                )

            payload = _decode_json_response(
                response,
                url,
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

    async def get(
        self,
        key: str,
    ):

        now = time.monotonic()

        async with self.lock:

            item = self.data.get(key)

            if item is None:
                return None

            expires_at, value = item

            if expires_at <= now:

                self.data.pop(
                    key,
                    None,
                )

                return None

            self.data.move_to_end(key)

            return value

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ):

        duration = (
            self.ttl
            if ttl is None
            else ttl
        )

        async with self.lock:

            self.data[key] = (
                time.monotonic() + duration,
                value,
            )

            self.data.move_to_end(key)

            while len(self.data) > self.max_size:

                self.data.popitem(
                    last=False
                )

    async def delete(
        self,
        key: str,
    ):

        async with self.lock:

            self.data.pop(
                key,
                None,
            )

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
    البيانات تضيع عند إعادة تشغيل التطبيق.
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

            self.data.pop(
                key,
                None,
            )

    async def update(
        self,
        key: str,
        **values,
    ) -> dict:

        async with self.lock:

            self._cleanup_locked()

            current = self.data.get(
                key,
                {},
            )

            record = {
                **current,
                **values,
                "updated_at": time.time(),
                "expires_at": (
                    time.monotonic() + self.ttl
                ),
            }

            self.data[key] = record

            self.data.move_to_end(key)

            while len(self.data) > self.max_size:

                self.data.popitem(
                    last=False
                )

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

            result.pop(
                "expires_at",
                None,
            )

            return result

    async def delete(
        self,
        key: str,
    ):

        async with self.lock:

            self.data.pop(
                key,
                None,
            )


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

    except (
        ValueError,
        TypeError,
        OverflowError,
    ):

        return default


def _clean_text(
    value: Any,
) -> str:

    if value is None:
        return ""

    text = str(value)

    text = unicodedata.normalize(
        "NFKC",
        text,
    )

    text = text.replace(
        "\u200b",
        "",
    )

    text = text.replace(
        "\ufeff",
        "",
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    )

    return text.strip()


# ==================================================
# TITLE NORMALIZATION
# ==================================================

def _normalize_title(
    value: Any,
) -> str:
    """
    تطبيع العناوين مع الحفاظ على الحروف
    العربية واليابانية والكورية والصينية وغيرها.
    """

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

    text = text.replace(
        "&",
        " and ",
    )

    characters = []

    for char in text:

        if char.isalnum():
            characters.append(char)

        elif char.isspace():
            characters.append(" ")

        else:
            characters.append(" ")

    text = "".join(characters)

    return re.sub(
        r"\s+",
        " ",
        text,
    ).strip()


def normalize_title(
    value: Any,
) -> str:

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

    intersection = len(
        words_a & words_b
    )

    union = len(
        words_a | words_b
    )

    jaccard_score = (
        intersection / union
        if union
        else 0.0
    )

    containment_score = (
        intersection
        / min(
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

    if not isinstance(attributes, dict):
        attributes = {}

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


# ==================================================
# LANGUAGE HELPERS
# ==================================================

def _language_name(
    code: str,
) -> str:

    code = str(
        code or ""
    ).lower().strip()

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

    data = payload.get(
        "data",
        [],
    )

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


# ==================================================
# CHAPTER ATTRIBUTE HELPERS
# ==================================================

def _chapter_attributes(
    chapter: dict,
) -> dict:

    if not isinstance(chapter, dict):
        return {}

    attributes = chapter.get("attributes", {})

    if not isinstance(attributes, dict):
        return {}

    return attributes


def _chapter_language(
    chapter: dict,
) -> Optional[str]:
    """
    إرجاع رمز اللغة المترجمة للفصل بصيغة موحَّدة.
    """

    attributes = _chapter_attributes(chapter)

    language = attributes.get("translatedLanguage")

    if not language:
        return None

    language = str(language).strip().lower()

    if not language:
        return None

    return language


def _chapter_number(
    chapter: dict,
) -> Optional[float]:
    """
    إرجاع رقم الفصل كـ float إن كان رقمياً،
    أو None للفصول الخاصة (Extras/Omake...).
    """

    attributes = _chapter_attributes(chapter)

    raw = attributes.get("chapter")

    if raw is None:
        return None

    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return None

    try:
        number = float(raw)

    except (ValueError, TypeError, OverflowError):
        return None

    if not math.isfinite(number):
        return None

    return number


def _chapter_title(
    chapter: dict,
) -> str:

    attributes = _chapter_attributes(chapter)

    return _clean_text(attributes.get("title"))


def _chapter_pages(
    chapter: dict,
) -> Optional[int]:

    attributes = _chapter_attributes(chapter)

    return _safe_int(attributes.get("pages"))


def _chapter_external_url(
    chapter: dict,
) -> Optional[str]:

    attributes = _chapter_attributes(chapter)

    url = attributes.get("externalUrl")

    if not url:
        return None

    url = str(url).strip()

    return url or None


def _chapter_published_at(
    chapter: dict,
) -> Optional[str]:

    attributes = _chapter_attributes(chapter)

    value = attributes.get("publishAt")

    if not value:
        return None

    value = str(value).strip()

    return value or None


def _chapter_scanlation_group_ids(
    chapter: dict,
) -> tuple[str, ...]:
    """
    استخراج معرّفات مجموعات الترجمة المرتبطة بالفصل.
    تُستخدم للتمييز بين نُسخ متعددة من نفس الفصل.
    """

    relationships = chapter.get("relationships", [])

    if not isinstance(relationships, list):
        return ()

    groups = []

    for relation in relationships:

        if not isinstance(relation, dict):
            continue

        if relation.get("type") != "scanlation_group":
            continue

        group_id = relation.get("id")

        if group_id:
            groups.append(str(group_id))

    return tuple(sorted(groups))


def _chapter_identity(
    chapter: dict,
) -> tuple:
    """
    هوية موحَّدة للفصل تُستخدم في إزالة التكرار.
    """

    attributes = _chapter_attributes(chapter)

    raw_volume = attributes.get("volume")
    volume_key = str(raw_volume) if raw_volume not in (None, "") else ""

    language = _chapter_language(chapter) or ""

    groups_key = ",".join(_chapter_scanlation_group_ids(chapter))

    number = _chapter_number(chapter)

    if number is not None:
        return (
            "number",
            volume_key,
            f"{number:g}",
            language,
            groups_key,
        )

    title = _chapter_title(chapter)

    if title:
        return (
            "special",
            volume_key,
            _normalize_title(title),
            language,
            groups_key,
        )

    return (
        "unique",
        str(chapter.get("id", "")),
    )


# ==================================================
# 🆕 MANGADEX-ONLY RESOLVER HELPERS
# ==================================================

def _build_resolved_result(
    manga: dict,
    source: str,
    match_score: Optional[float] = None,
) -> dict:
    """
    بناء استجابة موحّدة من كائن MangaDex (بدون AniList).
    """

    result = {
        "mangadex_id": manga.get("id"),
        "anilist_id": None,
        "title": _mangadex_title(manga),
        "titles": extract_titles(manga),
        "description": _mangadex_description(manga),
        "cover": _mangadex_cover(manga),
        "status": _mangadex_status(manga),
        "year": _mangadex_year(manga),
        "original_language": _mangadex_original_language(manga),
        "source": source,
        "mangadex": manga,
        "anilist": None,
    }

    if match_score is not None:
        result["match_score"] = round(float(match_score), 3)

    return result


async def search_mangadex(
    title: str,
    client: Optional[httpx.AsyncClient] = None,
    limit: int = 20,
) -> list[dict]:
    """
    البحث في MangaDex بالعنوان عبر GET /manga.
    """

    title = _clean_text(title)

    if not title:
        return []

    limit = max(1, min(int(limit), 100))

    cache_key = (
        f"search:{_normalize_title(title)}:{limit}"
    )

    cached = await _resolve_cache.get(cache_key)

    if cached is not None:

        return [
            dict(item)
            for item in cached
        ]

    params = {
        "title": title,
        "limit": limit,
        "includes[]": [
            "cover_art",
            "author",
            "artist",
        ],
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

    return [
        dict(item)
        for item in result
    ]


UUID_PATTERN = (
    r"[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}"
)


async def resolve_manga(
    manga_id: str,
    client: Optional[httpx.AsyncClient] = None,
) -> dict:
    """
    تحديد هوية المانجا من MangaDex فقط (بدون AniList).

    - UUID صالح → جلب مباشر من MangaDex.
    - أي نص آخر (اسم، عنوان، رقم) → بحث بالعنوان في MangaDex
      واختيار أفضل تطابق تلقائياً.
    """

    manga_id = str(manga_id).strip()

    if not manga_id:

        raise HTTPException(
            status_code=400,
            detail="معرّف المانجا مطلوب.",
        )

    cache_key = f"resolve:{manga_id}"

    cached = await _resolve_cache.get(cache_key)

    if cached is not None:
        return dict(cached)

    async def _fetch(active_client):

        # ==========================================
        # 1) UUID → جلب مباشر من MangaDex
        # ==========================================
        if re.fullmatch(UUID_PATTERN, manga_id):

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
                    detail=(
                        f"لا توجد مانجا في MangaDex بالمعرّف {manga_id}."
                    ),
                )

            return _build_resolved_result(
                manga,
                source="mangadex",
            )

        # ==========================================
        # 2) نص / عنوان → بحث في MangaDex
        # ==========================================
        logger.info(
            "Resolving manga by title via MangaDex: %s",
            manga_id,
        )

        try:
            candidates = await search_mangadex(
                manga_id,
                client=active_client,
                limit=20,
            )

        except HttpFailure as exc:

            raise HTTPException(
                status_code=502,
                detail=(
                    "فشل الاتصال بـ MangaDex أثناء البحث. "
                    f"تفاصيل: {exc}"
                ),
            ) from exc

        if not candidates:

            raise HTTPException(
                status_code=404,
                detail=(
                    f"لم يتم العثور على مانجا بعنوان «{manga_id}» "
                    "في MangaDex. تحقق من الاسم أو استخدم UUID."
                ),
            )

        # ترتيب أفضل التطابقات
        ranked = []

        for manga in candidates:

            if not isinstance(manga, dict):
                continue

            titles = extract_titles(manga)

            if not titles:
                continue

            score = _best_title_similarity(
                [manga_id],
                titles,
            )

            ranked.append((score, manga))

        if not ranked:

            raise HTTPException(
                status_code=404,
                detail=(
                    f"تم العثور على نتائج لـ «{manga_id}» "
                    "لكن بدون عناوين قابلة للمطابقة."
                ),
            )

        ranked.sort(
            key=lambda item: item[0],
            reverse=True,
        )

        best_score, best_manga = ranked[0]

        if best_score < MANGADEX_TITLE_MIN_SCORE:

            raise HTTPException(
                status_code=404,
                detail=(
                    f"تم العثور على نتائج لـ «{manga_id}» "
                    "لكن بدون تطابق موثوق. "
                    "جرّب اسماً أدق أو استخدم UUID مباشرة."
                ),
            )

        return _build_resolved_result(
            best_manga,
            source="mangadex-search",
            match_score=best_score,
        )

    try:

        if client is not None:

            result = await _fetch(client)

        else:

            async with _client_context() as active_client:
                result = await _fetch(active_client)

    except HTTPException:
        raise

    except HttpFailure as exc:

        raise _http_exception(exc) from exc

    await _resolve_cache.set(
        cache_key,
        result,
    )

    return result


# ==================================================
# END OF PART 1
# ==================================================
# ==================================================
# FETCH AVAILABLE LANGUAGES
# ==================================================

async def fetch_available_languages(
    manga_id: str,
    client: Optional[httpx.AsyncClient] = None,
) -> list[str]:
    """
    اكتشاف اللغات المتاحة لمانجا عبر /manga/{id}/feed
    (بدون فلتر لغة، ثم جمع translatedLanguage).
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
        total = None
        completed = False

        endpoint = f"/manga/{manga_id}/feed"

        for _page_index in range(MAX_FEED_PAGES):

            if offset in visited_offsets:

                raise HttpFailure(
                    "MangaDex language pagination loop detected",
                    status_code=502,
                )

            visited_offsets.add(offset)

            params = {
                "limit": FEED_PAGE_SIZE,
                "offset": offset,
                "order[chapter]": "asc",
            }

            payload = await _mangadex_get(
                active_client,
                endpoint,
                params,
            )

            chapters = _extract_feed_data(payload)

            page_total = _extract_feed_total(payload)

            if page_total is not None:
                total = page_total

            for chapter in chapters:

                language = _chapter_language(chapter)

                if language:
                    languages.add(language)

            if not chapters:

                if total is not None and offset < total:

                    raise HttpFailure(
                        "Incomplete MangaDex language pagination",
                        status_code=502,
                    )

                completed = True
                break

            offset += len(chapters)

            if total is not None and offset >= total:

                completed = True
                break

            if len(chapters) < FEED_PAGE_SIZE:

                if total is not None and offset < total:

                    raise HttpFailure(
                        "MangaDex returned an incomplete language page",
                        status_code=502,
                    )

                completed = True
                break

        if not completed:

            raise HttpFailure(
                "Maximum language pagination limit reached",
                status_code=502,
            )

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

    attributes = _chapter_attributes(chapter)

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

    title = _chapter_title(chapter).casefold()

    return (
        1,
        volume,
        title,
        str(raw_number or "").casefold(),
        str(chapter.get("id", "")),
    )


def chapter_sort_key(
    chapter: dict,
) -> tuple:

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

    return "special"


def classify_chapter(
    chapter: dict,
) -> str:

    return _chapter_type(chapter)


# ==================================================
# CHAPTER SERIALIZATION
# ==================================================

def _serialize_chapter(
    chapter: dict,
) -> dict:

    attributes = _chapter_attributes(chapter)

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
# FETCH ALL CHAPTERS  (يستعمل /manga/{id}/feed الرسمي)
# ==================================================

async def fetch_all_chapters(
    manga_id: str,
    language: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
    progress_key: Optional[str] = None,
) -> list[dict]:
    """
    جلب فصول مانجا من MangaDex عبر المسار الرسمي
    GET /manga/{id}/feed.

    - يدعم فلترة اللغة عبر translatedLanguage[].
    - يكشف الدوران والنتائج الناقصة.
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

        if progress_key:

            await _progress_store.update(
                progress_key,
                manga_id=manga_id,
                language=language,
                status="completed",
                chapters_fetched=len(cached),
                total=len(cached),
                page=0,
                cached=True,
            )

        return [
            dict(item)
            for item in cached
        ]

    async def _fetch(active_client):

        all_chapters = []

        offset = 0
        visited_offsets = set()

        total = None
        completed = False
        page_index = 0

        endpoint = f"/manga/{manga_id}/feed"

        for page_index in range(MAX_FEED_PAGES):

            if offset in visited_offsets:

                raise HttpFailure(
                    "MangaDex pagination loop detected",
                    status_code=502,
                )

            visited_offsets.add(offset)

            params = {
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
                endpoint,
                params,
            )

            page_chapters = _extract_feed_data(payload)

            page_total = _extract_feed_total(payload)

            if page_total is not None:
                total = page_total

            if not page_chapters:

                if total is not None and offset < total:

                    raise HttpFailure(
                        "Incomplete MangaDex feed: empty page before total",
                        status_code=502,
                    )

                completed = True
                break

            all_chapters.extend(page_chapters)

            previous_offset = offset

            offset += len(page_chapters)

            if offset <= previous_offset:

                raise HttpFailure(
                    "MangaDex pagination did not advance",
                    status_code=502,
                )

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

            if total is not None and offset >= total:

                completed = True
                break

            if len(page_chapters) < FEED_PAGE_SIZE:

                if total is not None and offset < total:

                    raise HttpFailure(
                        (
                            "Incomplete MangaDex feed: "
                            f"received {offset} of {total}"
                        ),
                        status_code=502,
                    )

                completed = True
                break

        if not completed:

            raise HttpFailure(
                "Maximum MangaDex pagination limit reached",
                status_code=502,
            )

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
                cached=False,
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

        logger.exception(
            "Unexpected chapter fetching error"
        )

        raise HttpFailure(
            f"Failed to fetch chapters: {exc}",
            status_code=502,
        ) from exc

    await _feed_cache.set(
        cache_key,
        result,
    )

    return [
        dict(item)
        for item in result
    ]


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

    alternatives = {}

    for chapter in chapters:

        attributes = _chapter_attributes(chapter)

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

        key = (
            f"{language or 'unknown'}:{identity}"
        )

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

            grouped.setdefault(
                code,
                [],
            ).append(chapter)

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
# MAIN CHAPTER ROUTE  (اللغة الافتراضية: ar)
# ==================================================

@router.get("/manga/{manga_id}/chapters")
async def get_chapters(
    manga_id: str,
    language: Optional[str] = Query(
        default=None,
        description="رمز اللغة (افتراضي: ar للعربية)",
    ),
):

    effective_language = language or DEFAULT_LANGUAGE

    normalized_language = _valid_language_code(
        effective_language
    )

    if not normalized_language:

        raise HTTPException(
            status_code=400,
            detail=(
                "رمز لغة غير صالح. استخدم رمزاً مثل ar أو en."
            ),
        )

    progress_key = f"{manga_id}:{normalized_language}"

    try:

        resolved = await resolve_manga(manga_id)

        actual_id = resolved.get("mangadex_id")

        if not actual_id:

            raise HTTPException(
                status_code=404,
                detail=(
                    "لم يتم العثور على معرّف MangaDex لهذه المانجا."
                ),
            )

        chapters = await fetch_all_chapters(
            actual_id,
            language=normalized_language,
            progress_key=progress_key,
        )

        if not chapters:

            lang_name = _language_name(normalized_language)

            raise HTTPException(
                status_code=404,
                detail=(
                    f"لا توجد فصول متاحة باللغة {lang_name} "
                    f"({normalized_language}) لهذه المانجا في MangaDex. "
                    "جرّب لغة أخرى أو تحقق لاحقاً."
                ),
            )

        response = _build_chapter_response(
            actual_id,
            chapters,
            normalized_language,
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
# LEGACY CHAPTER-LIST ROUTE  (اللغة الافتراضية: ar)
# ==================================================

@router.get("/manga/{manga_id}/chapter-list")
async def get_chapter_list(
    manga_id: str,
    language: Optional[str] = None,
):

    effective_language = language or DEFAULT_LANGUAGE

    normalized_language = _valid_language_code(
        effective_language
    )

    if not normalized_language:

        raise HTTPException(
            status_code=400,
            detail="رمز لغة غير صالح.",
        )

    progress_key = f"{manga_id}:{normalized_language}"

    try:

        resolved = await resolve_manga(manga_id)

        actual_id = resolved.get("mangadex_id")

        if not actual_id:

            raise HTTPException(
                status_code=404,
                detail="لم يتم العثور على معرّف MangaDex.",
            )

        chapters = await fetch_all_chapters(
            actual_id,
            language=normalized_language,
            progress_key=progress_key,
        )

        if not chapters:

            lang_name = _language_name(normalized_language)

            raise HTTPException(
                status_code=404,
                detail=(
                    f"لا توجد فصول باللغة {lang_name} "
                    f"({normalized_language}) لهذه المانجا."
                ),
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

    effective_language = language or DEFAULT_LANGUAGE

    normalized_language = _valid_language_code(
        effective_language
    )

    progress_key = (
        f"{manga_id}:{normalized_language or DEFAULT_LANGUAGE}"
    )

    progress = await _progress_store.snapshot(
        progress_key
    )

    if progress is None:

        return {
            "manga_id": manga_id,
            "language": normalized_language,
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
# CHAPTER LIST LANGUAGES
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
# END OF PART 2
# ==================================================
