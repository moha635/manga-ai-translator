"""
Manga AI Translator — Chapters Module
Version: 6.0.0 — Corrected Edition

البنية:
- Part 1: الأساس والمساعدات
- Part 2: مساعدات الفصول + Resolver + جلب الفصول
- Part 3: مسارات الفصول + الصور + MangaDex@Home
- Part 4: الترجمة + Job Management
- Part 5: SQLite + Cleanup + Health
- Part 6: ملف الاختبارات
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import sqlite3
import sys
import time
import unicodedata
import uuid

from abc import ABC, abstractmethod
from collections import OrderedDict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import httpx

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response


__version__ = "6.0.0"

router = APIRouter(prefix="/api/v1", tags=["Chapters"])


# ==================================================
# CONSTANTS
# ==================================================

MANGADEX_URL = "https://api.mangadex.org"

HEADERS = {
    "User-Agent": "MangaAITranslator/6.0.0",
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

HTTP_RETRY_ATTEMPTS = 3
HTTP_RETRY_BACKOFF = 0.6
MAX_RETRY_AFTER = 30.0

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

MAX_FEED_PAGES = 1000
FEED_PAGE_SIZE = 500

FEED_CACHE_TTL = 300
FEED_CACHE_MAX_SIZE = 300

LANGUAGES_CACHE_TTL = 900
LANGUAGES_CACHE_MAX_SIZE = 200

RESOLVE_CACHE_TTL = 600
RESOLVE_CACHE_MAX_SIZE = 500

PROGRESS_TTL = 600
PROGRESS_MAX_SIZE = 200

IMAGE_CACHE_TTL = 15 * 60
IMAGE_CACHE_MAX_SIZE = 1000

TITLE_MATCH_THRESHOLD = 0.90
TITLE_MATCH_MIN_MARGIN = 0.06
MANGADEX_TITLE_MIN_SCORE = 0.50

DEFAULT_LANGUAGE = "ar"

PREFERRED_SCANLATION_GROUPS: list[str] = []

logger = logging.getLogger(__name__)


# ==================================================
# SHARED HTTP CLIENT
# ==================================================

def _resolve_shared_client() -> Optional[httpx.AsyncClient]:
    for module_name in ("main", "backend.main"):
        module = sys.modules.get(module_name)
        if module is None:
            continue
        getter = getattr(module, "get_client", None)
        if not callable(getter):
            continue
        try:
            client = getter()
        except Exception:
            logger.exception("Could not retrieve shared HTTP client")
            continue
        if isinstance(client, httpx.AsyncClient) and not client.is_closed:
            return client
    return None


@asynccontextmanager
async def _client_context():
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
# ERRORS
# ==================================================

class HttpFailure(Exception):
    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable


def _http_exception(exc: HttpFailure) -> HTTPException:
    return HTTPException(
        status_code=exc.status_code or 502,
        detail=str(exc),
    )


# ==================================================
# RETRY HELPERS
# ==================================================

def parse_retry_after(value: Optional[str]) -> Optional[float]:
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
            retry_date = retry_date.replace(tzinfo=timezone.utc)
        return max(
            0.0,
            (retry_date - datetime.now(timezone.utc)).total_seconds(),
        )
    except (TypeError, ValueError, OverflowError):
        return None


async def _sleep_backoff(
    attempt: int,
    retry_after: Optional[float] = None,
) -> None:
    if retry_after is not None:
        delay = min(retry_after, MAX_RETRY_AFTER)
    else:
        delay = min(HTTP_RETRY_BACKOFF * (2 ** attempt), MAX_RETRY_AFTER)
    if delay > 0:
        await asyncio.sleep(delay)


# ==================================================
# HTTP RESPONSE HELPERS
# ==================================================

def _response_error_message(response: httpx.Response, url: str) -> str:
    status = response.status_code
    try:
        body = response.text[:500]
    except Exception:
        body = ""
    body = body.replace("\n", " ").strip()
    return f"HTTP {status} from {url}: {body}" if body else f"HTTP {status} from {url}"


def _decode_json_response(response: httpx.Response, url: str) -> dict:
    try:
        payload = response.json()
    except ValueError as exc:
        raise HttpFailure(f"استجابة JSON غير صالحة من {url}", 502) from exc
    if not isinstance(payload, dict):
        raise HttpFailure(f"صيغة الاستجابة غير متوقعة من {url}", 502)
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

    for attempt in range(HTTP_RETRY_ATTEMPTS):
        response = None
        try:
            response = await client.get(url, params=params, timeout=REQUEST_TIMEOUT)
            status = response.status_code

            if status in RETRYABLE_STATUS_CODES:
                retry_after = parse_retry_after(response.headers.get("Retry-After"))
                if attempt < HTTP_RETRY_ATTEMPTS - 1:
                    await response.aclose()
                    response = None
                    await _sleep_backoff(attempt, retry_after)
                    continue
                raise HttpFailure(
                    _response_error_message(response, url),
                    status_code=status,
                    retryable=True,
                )

            if status >= 400:
                raise HttpFailure(_response_error_message(response, url), status)

            return status, _decode_json_response(response, url)

        except HttpFailure:
            raise

        except httpx.TimeoutException as exc:
            last_error = exc
            if attempt >= HTTP_RETRY_ATTEMPTS - 1:
                raise HttpFailure(
                    f"انتهت مهلة الاتصال: {url}", 504, retryable=True
                ) from exc
            await _sleep_backoff(attempt)

        except httpx.RequestError as exc:
            last_error = exc
            if attempt >= HTTP_RETRY_ATTEMPTS - 1:
                raise HttpFailure(
                    f"تعذر الاتصال: {url}", 502, retryable=True
                ) from exc
            await _sleep_backoff(attempt)

        finally:
            if response is not None:
                await response.aclose()

    raise HttpFailure(f"فشل الطلب: {last_error}", 502, retryable=True)


# ==================================================
# TTL CACHE
# ==================================================

class _TTLCache:
    def __init__(self, ttl: int, max_size: int = 200) -> None:
        self.ttl = ttl
        self.max_size = max_size
        self.data: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self.lock = asyncio.Lock()

    async def get(self, key: str) -> Any:
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
    ) -> None:
        duration = self.ttl if ttl is None else ttl
        async with self.lock:
            self.data[key] = (time.monotonic() + duration, value)
            self.data.move_to_end(key)
            while len(self.data) > self.max_size:
                self.data.popitem(last=False)

    async def delete(self, key: str) -> None:
        async with self.lock:
            self.data.pop(key, None)

    async def clear(self) -> None:
        async with self.lock:
            self.data.clear()

    async def cleanup(self) -> int:
        """حذف العناصر المنتهية. يُعيد عدد المحذوفة."""
        now = time.monotonic()
        removed = 0
        async with self.lock:
            expired = [
                k for k, (exp, _) in self.data.items() if exp <= now
            ]
            for k in expired:
                self.data.pop(k, None)
                removed += 1
        return removed


# ==================================================
# CACHES
# ==================================================

_feed_cache = _TTLCache(FEED_CACHE_TTL, FEED_CACHE_MAX_SIZE)
_languages_cache = _TTLCache(LANGUAGES_CACHE_TTL, LANGUAGES_CACHE_MAX_SIZE)
_resolve_cache = _TTLCache(RESOLVE_CACHE_TTL, RESOLVE_CACHE_MAX_SIZE)
_image_cache = _TTLCache(IMAGE_CACHE_TTL, IMAGE_CACHE_MAX_SIZE)


# ==================================================
# PROGRESS STORE
# ==================================================

class _ProgressStore:
    def __init__(self, ttl: int = PROGRESS_TTL, max_size: int = PROGRESS_MAX_SIZE) -> None:
        self.ttl = ttl
        self.max_size = max_size
        self.data: OrderedDict[str, dict] = OrderedDict()
        self.lock = asyncio.Lock()

    def _cleanup_locked(self) -> None:
        now = time.monotonic()
        expired = [k for k, v in self.data.items() if v["expires_at"] <= now]
        for k in expired:
            self.data.pop(k, None)

    async def update(self, key: str, **values: Any) -> dict:
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

    async def snapshot(self, key: str) -> Optional[dict]:
        async with self.lock:
            self._cleanup_locked()
            item = self.data.get(key)
            if item is None:
                return None
            self.data.move_to_end(key)
            result = dict(item)
            result.pop("expires_at", None)
            return result

    async def delete(self, key: str) -> None:
        async with self.lock:
            self.data.pop(key, None)


_progress_store = _ProgressStore()


# ==================================================
# GENERAL HELPERS
# ==================================================

def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError, OverflowError):
        return default


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = text.replace("\u200b", "").replace("\ufeff", "")
    return re.sub(r"\s+", " ", text).strip()


# ==================================================
# TITLE HELPERS
# ==================================================

def _normalize_title(value: Any) -> str:
    text = _clean_text(value).casefold()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.replace("&", " and ")

    characters = []
    for char in text:
        if char.isalnum():
            characters.append(char)
        elif char.isspace():
            characters.append(" ")
        else:
            characters.append(" ")

    return re.sub(r"\s+", " ", "".join(characters)).strip()


def normalize_title(value: Any) -> str:
    return _normalize_title(value)


def title_similarity(first: Any, second: Any) -> float:
    a = _normalize_title(first)
    b = _normalize_title(second)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0

    sequence_score = SequenceMatcher(None, a, b).ratio()
    words_a = set(a.split())
    words_b = set(b.split())

    if not words_a or not words_b:
        return sequence_score

    intersection = len(words_a & words_b)
    union = len(words_a | words_b)
    jaccard_score = intersection / union if union else 0.0
    containment_score = intersection / min(len(words_a), len(words_b))

    return max(sequence_score, jaccard_score, containment_score * 0.85)


def extract_titles(manga: dict) -> list[str]:
    titles = []
    if not isinstance(manga, dict):
        return titles

    attributes = manga.get("attributes", {})
    if not isinstance(attributes, dict):
        return titles

    title_data = attributes.get("title")
    if isinstance(title_data, dict):
        for value in title_data.values():
            cleaned = _clean_text(value)
            if cleaned:
                titles.append(cleaned)

    alt_titles = attributes.get("altTitles", [])
    if isinstance(alt_titles, list):
        for item in alt_titles:
            if not isinstance(item, dict):
                continue
            for value in item.values():
                cleaned = _clean_text(value)
                if cleaned:
                    titles.append(cleaned)

    result, seen = [], set()
    for title in titles:
        normalized = _normalize_title(title)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(title)
    return result


def _best_title_similarity(titles_a: Iterable[str], titles_b: Iterable[str]) -> float:
    best = 0.0
    for ta in titles_a:
        for tb in titles_b:
            score = title_similarity(ta, tb)
            if score > best:
                best = score
            if best >= 1.0:
                return 1.0
    return best


# ==================================================
# MANGADEX METADATA HELPERS
# ==================================================

def _mangadex_title(manga: dict) -> str:
    titles = extract_titles(manga)
    if not titles:
        return "Unknown"

    attributes = manga.get("attributes", {})
    if isinstance(attributes, dict):
        primary = attributes.get("title", {})
        if isinstance(primary, dict):
            for lang in ("en", "ja-ro", "ja"):
                value = _clean_text(primary.get(lang))
                if value:
                    return value
    return titles[0]


def _mangadex_description(manga: dict) -> str:
    attributes = manga.get("attributes", {})
    if not isinstance(attributes, dict):
        return ""
    desc = attributes.get("description", {})
    if not isinstance(desc, dict):
        return ""
    for lang in ("en", "ar", "fr"):
        value = _clean_text(desc.get(lang))
        if value:
            return value
    for value in desc.values():
        cleaned = _clean_text(value)
        if cleaned:
            return cleaned
    return ""


def _mangadex_cover(manga: dict) -> Optional[str]:
    relations = manga.get("relationships", [])
    if not isinstance(relations, list):
        return None
    for rel in relations:
        if not isinstance(rel, dict) or rel.get("type") != "cover_art":
            continue
        cover_id = rel.get("id")
        if not cover_id:
            continue
        attrs = rel.get("attributes", {})
        filename = attrs.get("fileName") if isinstance(attrs, dict) else None
        if filename:
            return f"https://uploads.mangadex.org/covers/{manga.get('id')}/{filename}"
    return None


def _mangadex_status(manga: dict) -> Optional[str]:
    attrs = manga.get("attributes", {})
    if not isinstance(attrs, dict):
        return None
    status = attrs.get("status")
    return str(status).strip().lower() if status else None


def _mangadex_year(manga: dict) -> Optional[int]:
    attrs = manga.get("attributes", {})
    if not isinstance(attrs, dict):
        return None
    return _safe_int(attrs.get("year"))


def _mangadex_original_language(manga: dict) -> Optional[str]:
    attrs = manga.get("attributes", {})
    if not isinstance(attrs, dict):
        return None
    language = attrs.get("originalLanguage")
    return str(language).strip().lower() if language else None


# ==================================================
# MANGADEX GET (يعيد dict فقط)
# ==================================================

async def _mangadex_get(
    client: httpx.AsyncClient,
    endpoint: str,
    params: Optional[dict] = None,
) -> dict:
    url = endpoint if endpoint.startswith("http") else f"{MANGADEX_URL}{endpoint}"
    _, payload = await _http_get_json(client, url, params=params)
    return payload


# ==================================================
# LANGUAGE HELPERS
# ==================================================

def _language_name(code: str) -> str:
    code = str(code or "").lower().strip()
    return LANGUAGES.get(code, code)


def _valid_language_code(code: Any) -> Optional[str]:
    if not isinstance(code, str):
        return None
    code = code.strip().lower()
    if not code:
        return None
    if not re.fullmatch(r"[a-z]{2,3}(?:-[a-z0-9]{2,8})?", code):
        return None
    return code


# ==================================================
# FEED HELPERS
# ==================================================

def _extract_feed_data(payload: dict) -> list[dict]:
    data = payload.get("data", [])
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def _extract_feed_total(payload: dict) -> Optional[int]:
    return _safe_int(payload.get("total"))


# ============ END OF PART 1 ============
# ============ PART 2: CHAPTER HELPERS + RESOLVER + FETCH ============

# ==================================================
# CHAPTER HELPERS
# ==================================================

def _chapter_attributes(chapter: dict) -> dict:
    if not isinstance(chapter, dict):
        return {}
    attrs = chapter.get("attributes", {})
    return attrs if isinstance(attrs, dict) else {}


def _chapter_language(chapter: dict) -> Optional[str]:
    language = _chapter_attributes(chapter).get("translatedLanguage")
    if not language:
        return None
    return str(language).strip().lower() or None


def _chapter_number(chapter: dict) -> Optional[float]:
    raw = _chapter_attributes(chapter).get("chapter")
    if raw is None:
        return None
    try:
        number = float(str(raw).strip())
    except (ValueError, TypeError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _chapter_title(chapter: dict) -> str:
    return _clean_text(_chapter_attributes(chapter).get("title"))


def _chapter_pages(chapter: dict) -> Optional[int]:
    return _safe_int(_chapter_attributes(chapter).get("pages"))


def _chapter_external_url(chapter: dict) -> Optional[str]:
    url = _chapter_attributes(chapter).get("externalUrl")
    if not url:
        return None
    return str(url).strip() or None


def _chapter_published_at(chapter: dict) -> Optional[str]:
    value = _chapter_attributes(chapter).get("publishAt")
    if not value:
        return None
    return str(value).strip() or None


def _chapter_scanlation_group_ids(chapter: dict) -> tuple[str, ...]:
    relationships = chapter.get("relationships", [])
    if not isinstance(relationships, list):
        return ()
    groups = []
    for rel in relationships:
        if not isinstance(rel, dict):
            continue
        if rel.get("type") != "scanlation_group":
            continue
        group_id = rel.get("id")
        if group_id:
            groups.append(str(group_id))
    return tuple(sorted(groups))


def _chapter_identity(chapter: dict) -> tuple:
    """
    هوية الفصل: (volume, number/title, language).

    لا يدخل في المفتاح:
    - scanlation_group (لأننا نريد اختيار نسخة واحدة)
    - chapter_id (لأننا نريد تجميع النسخ)
    """
    attrs = _chapter_attributes(chapter)
    volume = str(attrs.get("volume") or "")
    language = _chapter_language(chapter) or ""
    number = _chapter_number(chapter)

    if number is not None:
        return ("number", volume, number, language)

    title = _chapter_title(chapter)
    if title:
        return ("special", volume, _normalize_title(title), language)

    return ("unique", str(chapter.get("id", "")))


def _chapter_type(chapter: dict) -> str:
    return "numbered" if _chapter_number(chapter) is not None else "special"


def classify_chapter(chapter: dict) -> str:
    return _chapter_type(chapter)


def _chapter_sort_key(chapter: dict) -> tuple:
    """
    ترتيب تصاعدي: volume ثم chapter_number.
    الفصول الخاصة (بدون رقم) في النهاية مرتّبة بالعنوان.
    """
    attrs = _chapter_attributes(chapter)
    number = _chapter_number(chapter)
    volume = _safe_int(attrs.get("volume"), -1) or -1

    if number is not None:
        return (0, volume, number, str(chapter.get("id", "")))

    return (
        1,
        volume,
        _chapter_title(chapter).casefold(),
        str(chapter.get("id", "")),
    )


def chapter_sort_key(chapter: dict) -> tuple:
    return _chapter_sort_key(chapter)


def _deduplicate_chapters(chapters: Iterable[dict]) -> list[dict]:
    """
    إزالة التكرار: اختيار نسخة واحدة لكل فصل.

    القواعد:
    1. تفضيل الأحدث publishAt (مقارنة نصية ISO قابلة للمقارنة).
    2. عند التعادل: تفضيل مجموعة Scanlation المفضلة.
    3. عند التعادل: تفضيل أول ظهور.
    """
    selected: dict[tuple, dict] = {}

    for chapter in chapters:
        if not isinstance(chapter, dict):
            continue

        identity = _chapter_identity(chapter)
        current = selected.get(identity)

        if current is None:
            selected[identity] = chapter
            continue

        candidate_pub = _chapter_published_at(chapter) or ""
        current_pub = _chapter_published_at(current) or ""

        # الأحدث تاريخاً يفوز (ISO 8601 يدعم المقارنة النصية)
        if candidate_pub > current_pub:
            selected[identity] = chapter
            continue
        if candidate_pub < current_pub:
            continue

        # تواريخ متساوية → تفضيل مجموعة الترجمة المفضلة
        candidate_preferred = any(
            g in PREFERRED_SCANLATION_GROUPS
            for g in _chapter_scanlation_group_ids(chapter)
        )
        current_preferred = any(
            g in PREFERRED_SCANLATION_GROUPS
            for g in _chapter_scanlation_group_ids(current)
        )
        if candidate_preferred and not current_preferred:
            selected[identity] = chapter

    return list(selected.values())


def _serialize_chapter(chapter: dict) -> dict:
    attrs = _chapter_attributes(chapter)
    chapter_id = chapter.get("id")
    language = _chapter_language(chapter)
    external_url = _chapter_external_url(chapter)
    published_at = _chapter_published_at(chapter)

    return {
        "id": chapter_id,
        "chapter_id": chapter_id,
        "chapter": attrs.get("chapter"),
        "volume": attrs.get("volume"),
        "title": _chapter_title(chapter),
        "translatedLanguage": language,
        "language": language,
        "language_name": _language_name(language) if language else None,
        "pages": _chapter_pages(chapter),
        "publishAt": published_at,
        "published_at": published_at,
        "externalUrl": external_url,
        "external_url": external_url,
        "type": _chapter_type(chapter),
        "is_translatable": not bool(external_url),
        "url": (
            f"https://mangadex.org/chapter/{chapter_id}"
            if chapter_id
            else external_url
        ),
    }


# ==================================================
# RESOLVER
# ==================================================

def _build_resolved_result(
    manga: dict,
    source: str,
    match_score: Optional[float] = None,
) -> dict:
    result = {
        "mangadex_id": manga.get("id"),
        "title": _mangadex_title(manga),
        "titles": extract_titles(manga),
        "description": _mangadex_description(manga),
        "cover": _mangadex_cover(manga),
        "status": _mangadex_status(manga),
        "year": _mangadex_year(manga),
        "original_language": _mangadex_original_language(manga),
        "source": source,
        "mangadex": manga,
    }
    if match_score is not None:
        result["match_score"] = round(float(match_score), 3)
    return result


UUID_PATTERN = (
    r"[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}"
)


async def search_mangadex(
    title: str,
    client: Optional[httpx.AsyncClient] = None,
    limit: int = 20,
) -> list[dict]:
    title = _clean_text(title)
    if not title:
        return []

    limit = max(1, min(int(limit), 100))
    cache_key = f"search:{_normalize_title(title)}:{limit}"

    cached = await _resolve_cache.get(cache_key)
    if cached is not None:
        return [dict(item) for item in cached]

    params = {
        "title": title,
        "limit": limit,
        "includes[]": ["cover_art", "author", "artist"],
        "order[relevance]": "desc",
    }

    async def _fetch(active_client: httpx.AsyncClient):
        payload = await _mangadex_get(active_client, "/manga", params)
        return _extract_feed_data(payload)

    if client is not None:
        result = await _fetch(client)
    else:
        async with _client_context() as active_client:
            result = await _fetch(active_client)

    await _resolve_cache.set(cache_key, result)
    return [dict(item) for item in result]


async def resolve_manga(
    manga_id: str,
    client: Optional[httpx.AsyncClient] = None,
) -> dict:
    """
    تحديد هوية المانجا من MangaDex فقط.

    - UUID → جلب مباشر.
    - نص/عنوان → بحث + أفضل تطابق.
    """
    manga_id = str(manga_id).strip()
    if not manga_id:
        raise HTTPException(400, "معرّف المانجا مطلوب.")

    cache_key = f"resolve:{manga_id}"
    cached = await _resolve_cache.get(cache_key)
    if cached is not None:
        return dict(cached)

    async def _fetch(active_client: httpx.AsyncClient):
        # 1) UUID → MangaDex مباشرة
        if re.fullmatch(UUID_PATTERN, manga_id):
            payload = await _mangadex_get(
                active_client,
                f"/manga/{manga_id}",
                {"includes[]": ["cover_art", "author", "artist"]},
            )
            manga = payload.get("data")
            if not isinstance(manga, dict):
                raise HTTPException(404, "لم يتم العثور على المانجا.")
            return _build_resolved_result(manga, source="mangadex")

        # 2) نص/عنوان → بحث
        logger.info("Resolving manga by title: %s", manga_id)
        candidates = await search_mangadex(
            manga_id, client=active_client, limit=20
        )

        if not candidates:
            raise HTTPException(
                404, f"لم يتم العثور على مانجا بعنوان «{manga_id}»."
            )

        ranked = []
        for manga in candidates:
            titles = extract_titles(manga)
            if not titles:
                continue
            score = _best_title_similarity([manga_id], titles)
            ranked.append((score, manga))

        if not ranked:
            raise HTTPException(404, "نتائج البحث بدون عناوين قابلة للمطابقة.")

        ranked.sort(key=lambda x: x[0], reverse=True)
        best_score, best_manga = ranked[0]

        if best_score < MANGADEX_TITLE_MIN_SCORE:
            raise HTTPException(
                404,
                "لم يتم العثور على تطابق موثوق. جرّب اسمًا أدق أو UUID.",
            )

        if (
            len(ranked) > 1
            and best_score >= TITLE_MATCH_THRESHOLD
            and ranked[1][0] >= TITLE_MATCH_THRESHOLD
            and (best_score - ranked[1][0]) < TITLE_MATCH_MIN_MARGIN
        ):
            raise HTTPException(
                409,
                "عُثر على أكثر من مانجا متشابهة. استخدم UUID.",
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

    await _resolve_cache.set(cache_key, result)
    return result


# ==================================================
# FETCH AVAILABLE LANGUAGES
# ==================================================

async def fetch_available_languages(
    manga_id: str,
    client: Optional[httpx.AsyncClient] = None,
) -> list[str]:
    manga_id = str(manga_id).strip()
    if not manga_id:
        return []

    cached = await _languages_cache.get(manga_id)
    if cached is not None:
        return list(cached)

    async def _fetch(active_client: httpx.AsyncClient):
        languages = set()
        offset = 0
        total = None
        page_count = 0
        visited_offsets = set()

        while True:
            if offset in visited_offsets:
                raise HttpFailure("تم اكتشاف دوران في جلب اللغات.", 502)
            visited_offsets.add(offset)

            page_count += 1
            if page_count > MAX_FEED_PAGES:
                raise HttpFailure("تم تجاوز الحد الأقصى للصفحات.", 502)

            params = {
                "limit": FEED_PAGE_SIZE,
                "offset": offset,
                "order[chapter]": "asc",
            }

            payload = await _mangadex_get(
                active_client,
                f"/manga/{manga_id}/feed",
                params,
            )
            chapters = _extract_feed_data(payload)
            page_total = _extract_feed_total(payload)

            if page_total is not None:
                total = page_total

            for ch in chapters:
                lang = _chapter_language(ch)
                if lang:
                    languages.add(lang)

            if not chapters:
                break

            prev = offset
            offset += len(chapters)
            if offset <= prev:
                raise HttpFailure("لم يتقدم المؤشر.", 502)

            if total is not None and offset >= total:
                break
            if total is None and len(chapters) < FEED_PAGE_SIZE:
                break

        return sorted(languages)

    if client is not None:
        result = await _fetch(client)
    else:
        async with _client_context() as active_client:
            result = await _fetch(active_client)

    await _languages_cache.set(manga_id, result)
    return result


# ==================================================
# FETCH ALL CHAPTERS
# ==================================================

async def fetch_all_chapters(
    manga_id: str,
    language: Optional[str] = None,
    client: Optional[httpx.AsyncClient] = None,
    progress_key: Optional[str] = None,
    include_external: bool = False,
) -> list[dict]:
    """
    جلب جميع فصول المانجا عبر /manga/{id}/feed.

    - يكتشف الدوران والنتائج الناقصة.
    - يزيل التكرار ويُرتّب تصاعدياً.
    - يستبعد externalUrl افتراضياً.
    """
    manga_id = str(manga_id).strip()
    if not manga_id:
        raise HttpFailure("معرّف MangaDex مطلوب.", 400)

    language = _valid_language_code(language) if language else None

    cache_key = f"{manga_id}:{language or 'all'}:{int(include_external)}"
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
        return [dict(item) for item in cached]

    async def _fetch(active_client: httpx.AsyncClient):
        all_chapters: list[dict] = []
        offset = 0
        total = None
        visited_offsets = set()
        page_index = 0
        endpoint = f"/manga/{manga_id}/feed"

        while True:
            if offset in visited_offsets:
                raise HttpFailure("تم اكتشاف دوران في ترقيم الصفحات.", 502)
            if page_index >= MAX_FEED_PAGES:
                raise HttpFailure("تم بلوغ الحد الأقصى للصفحات.", 502)

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

            payload = await _mangadex_get(active_client, endpoint, params)
            page_chapters = _extract_feed_data(payload)
            page_total = _extract_feed_total(payload)

            if page_total is not None:
                total = page_total

            logger.info(
                "MangaDex feed: manga=%s offset=%s total=%s page_len=%s",
                manga_id,
                offset,
                total,
                len(page_chapters),
            )

            if not page_chapters:
                if total is not None and offset < total:
                    raise HttpFailure(
                        f"انتهى المصدر قبل جلب جميع الفصول: {offset}/{total}.",
                        502,
                    )
                break

            all_chapters.extend(page_chapters)
            prev = offset
            offset += len(page_chapters)
            page_index += 1

            if offset <= prev:
                raise HttpFailure("لم يتقدم المؤشر.", 502)

            if progress_key:
                await _progress_store.update(
                    progress_key,
                    manga_id=manga_id,
                    language=language,
                    status="running",
                    page=page_index,
                    offset=offset,
                    chapters_fetched=len(all_chapters),
                    total=total,
                )

            if total is not None:
                if offset >= total:
                    break
            elif len(page_chapters) < FEED_PAGE_SIZE:
                break

        if total is not None and len(all_chapters) < total:
            raise HttpFailure(
                f"نتائج غير مكتملة: {len(all_chapters)}/{total}.",
                502,
            )

        raw_count = len(all_chapters)
        all_chapters = _deduplicate_chapters(all_chapters)
        dedup_count = len(all_chapters)
        all_chapters.sort(key=_chapter_sort_key)

        if not include_external:
            all_chapters = [
                c for c in all_chapters if not _chapter_external_url(c)
            ]

        logger.info(
            "MangaDex chapters: manga=%s raw=%s dedup=%s final=%s total=%s",
            manga_id,
            raw_count,
            dedup_count,
            len(all_chapters),
            total,
        )

        if progress_key:
            await _progress_store.update(
                progress_key,
                manga_id=manga_id,
                language=language,
                status="completed",
                page=page_index,
                offset=offset,
                chapters_fetched=len(all_chapters),
                raw_chapters_fetched=raw_count,
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
        logger.exception("خطأ غير متوقع أثناء جلب الفصول")
        raise HttpFailure(f"فشل جلب الفصول: {exc}", 502) from exc

    await _feed_cache.set(cache_key, result)
    return [dict(item) for item in result]


# ============ END OF PART 2 ============
# ============ PART 3: CHAPTER ROUTES + IMAGES + MANGADEX@HOME ============

# ==================================================
# CHAPTER LIST ROUTES (القديمة + التشخيصية)
# ==================================================

def _chapter_counts(chapters: Iterable[dict]) -> dict:
    """إحصاءات سريعة على قائمة فصول."""
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
        lang = _chapter_language(chapter)
        if lang:
            languages.add(lang)

    return {
        "total": total,
        "numbered": numbered,
        "special": special,
        "languages": sorted(languages),
        "language_count": len(languages),
    }


def _build_chapter_response(
    manga_id: str,
    chapters: list[dict],
    language: Optional[str] = None,
) -> dict:
    serialized = [_serialize_chapter(ch) for ch in chapters]

    grouped: dict[str, list[dict]] = {}
    for ch in serialized:
        code = ch.get("language")
        if code:
            grouped.setdefault(code, []).append(ch)

    return {
        "manga_id": manga_id,
        "language": language,
        "total": len(serialized),
        "chapters": serialized,
        "languages": sorted(grouped.keys()),
        "chapters_by_language": grouped,
        "counts": _chapter_counts(chapters),
    }


@router.get("/manga/{manga_id}/chapters")
async def get_chapters(
    manga_id: str,
    language: Optional[str] = Query(
        default=None,
        description="رمز اللغة (افتراضي: ar)",
    ),
):
    """المسار الرئيسي: يحل المانجا ويجلب فصولها باللغة المطلوبة."""
    effective_language = language or DEFAULT_LANGUAGE
    normalized_language = _valid_language_code(effective_language)

    if not normalized_language:
        raise HTTPException(
            400,
            "رمز لغة غير صالح. استخدم رمزاً مثل ar أو en.",
        )

    progress_key = f"{manga_id}:{normalized_language}"

    try:
        resolved = await resolve_manga(manga_id)
        actual_id = resolved.get("mangadex_id")

        if not actual_id:
            raise HTTPException(404, "لم يتم العثور على معرّف MangaDex.")

        chapters = await fetch_all_chapters(
            actual_id,
            language=normalized_language,
            progress_key=progress_key,
        )

        if not chapters:
            lang_name = _language_name(normalized_language)
            raise HTTPException(
                404,
                f"لا توجد فصول متاحة باللغة {lang_name} "
                f"({normalized_language}) لهذه المانجا في MangaDex. "
                "جرّب لغة أخرى أو تحقق لاحقاً.",
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


@router.get("/manga/{manga_id}/chapter-list")
async def get_chapter_list(
    manga_id: str,
    language: Optional[str] = None,
):
    """نسخة متوافقة: نفس سلوك /chapters لكن بحقول أقل."""
    effective_language = language or DEFAULT_LANGUAGE
    normalized_language = _valid_language_code(effective_language)

    if not normalized_language:
        raise HTTPException(400, "رمز لغة غير صالح.")

    progress_key = f"{manga_id}:{normalized_language}"

    try:
        resolved = await resolve_manga(manga_id)
        actual_id = resolved.get("mangadex_id")

        if not actual_id:
            raise HTTPException(404, "لم يتم العثور على معرّف MangaDex.")

        chapters = await fetch_all_chapters(
            actual_id,
            language=normalized_language,
            progress_key=progress_key,
        )

        if not chapters:
            lang_name = _language_name(normalized_language)
            raise HTTPException(
                404,
                f"لا توجد فصول باللغة {lang_name} ({normalized_language}).",
            )

        serialized = [_serialize_chapter(ch) for ch in chapters]

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


@router.get("/manga/{manga_id}/chapter-list/progress")
async def get_chapter_list_progress(
    manga_id: str,
    language: Optional[str] = None,
):
    """حالة تقدم جلب الفصول من الذاكرة."""
    effective_language = language or DEFAULT_LANGUAGE
    normalized_language = _valid_language_code(effective_language)

    progress_key = (
        f"{manga_id}:{normalized_language or DEFAULT_LANGUAGE}"
    )

    progress = await _progress_store.snapshot(progress_key)

    if progress is None:
        return {
            "manga_id": manga_id,
            "language": normalized_language,
            "status": "not_started",
            "chapters_fetched": 0,
            "page": 0,
            "total": None,
        }

    return {"manga_id": manga_id, **progress}


@router.get("/manga/{manga_id}/chapter-list/languages")
async def get_chapter_list_languages(manga_id: str):
    """اللغات المتاحة فعلًا لهذه المانجا."""
    try:
        resolved = await resolve_manga(manga_id)
        actual_id = resolved.get("mangadex_id")

        if not actual_id:
            raise HTTPException(404, "MangaDex ID not found")

        languages = await fetch_available_languages(actual_id)

        return {
            "manga_id": actual_id,
            "languages": [
                {"code": code, "name": _language_name(code)}
                for code in languages
            ],
            "total": len(languages),
        }

    except HTTPException:
        raise
    except HttpFailure as exc:
        raise _http_exception(exc) from exc


# ==================================================
# DIAGNOSTIC ROUTES (تشخيص 24 فصل)
# ==================================================

@router.get("/manga/{manga_id}/chapters/count")
async def get_chapters_count(
    manga_id: str,
    language: Optional[str] = None,
):
    """
    عدد الفصول الإجمالي حسب MangaDex (بدون جلبها).
    يقارن total من API مع ما سيُجلب فعليًا.
    """
    try:
        resolved = await resolve_manga(manga_id)
        actual_id = resolved.get("mangadex_id")

        if not actual_id:
            raise HTTPException(404, "MangaDex ID not found")

        lang = _valid_language_code(language) if language else DEFAULT_LANGUAGE

        params: dict = {"limit": 1, "offset": 0}
        if lang:
            params["translatedLanguage[]"] = [lang]

        async with _client_context() as client:
            payload = await _mangadex_get(
                client,
                f"/manga/{actual_id}/feed",
                params,
            )

        total = _extract_feed_total(payload)

        return {
            "manga_id": actual_id,
            "language": lang,
            "total_from_mangadex": total,
            "note": (
                "العدد الإجمالي بدون إزالة تكرار أو استبعاد externalUrl. "
                "استخدم /chapters/debug للتفصيل الكامل."
            ),
        }

    except HTTPException:
        raise
    except HttpFailure as exc:
        raise _http_exception(exc) from exc


@router.get("/manga/{manga_id}/chapters/debug")
async def debug_chapters(
    manga_id: str,
    language: Optional[str] = None,
):
    """
    تشخيص مفصل: يفكك مسار الجلب خطوة بخطوة.
    يحسم مسألة "لماذا 24 فصلًا فقط؟" بشكل قاطع.
    """
    try:
        resolved = await resolve_manga(manga_id)
        actual_id = resolved.get("mangadex_id")

        if not actual_id:
            raise HTTPException(404, "MangaDex ID not found")

        lang = _valid_language_code(language) if language else DEFAULT_LANGUAGE

        # 1) total من MangaDex
        params: dict = {"limit": 1, "offset": 0}
        if lang:
            params["translatedLanguage[]"] = [lang]

        async with _client_context() as client:
            head = await _mangadex_get(
                client,
                f"/manga/{actual_id}/feed",
                params,
            )
        total_from_api = _extract_feed_total(head)

        # 2) جلب فعلي (بدون فلترة external)
        raw = await fetch_all_chapters(
            actual_id,
            language=lang,
            include_external=True,
        )
        raw_count = len(raw)

        # 3) عدّ externalUrl
        external_count = sum(
            1 for c in raw if _chapter_external_url(c)
        )

        # 4) بعد إزالة التكرار
        deduped = _deduplicate_chapters(raw)
        deduped_count = len(deduped)

        # 5) بعد استبعاد external
        final = [c for c in deduped if not _chapter_external_url(c)]
        final_count = len(final)

        # 6) تشخيص
        diagnosis = {
            "total_from_api": total_from_api,
            "raw_fetched": raw_count,
            "external_url_count": external_count,
            "after_dedup": deduped_count,
            "final_returned": final_count,
            "matches_api_total": raw_count == total_from_api,
            "dedup_removed": raw_count - deduped_count,
            "external_removed_by_filter": external_count,
            "explanation": (
                "إذا raw_fetched == total_from_api → الـ Pagination سليم، "
                "والعدد النهائي أقل بسبب إزالة التكرار أو استبعاد external. "
                "إذا raw_fetched < total_from_api → هناك bug في Pagination."
            ),
        }

        return {
            "manga_id": actual_id,
            "language": lang,
            "diagnosis": diagnosis,
        }

    except HTTPException:
        raise
    except HttpFailure as exc:
        raise _http_exception(exc) from exc


# ==================================================
# IMAGE FETCHING (MangaDex@Home)
# ==================================================

class ChapterImageError(Exception):
    """خطأ أثناء جلب صور الفصل."""


def _image_error(message: str, status_code: int = 502) -> HTTPException:
    return HTTPException(status_code=status_code, detail=message)


# Lazy semaphore للتحميل المتوازي
_image_semaphore: Optional[asyncio.Semaphore] = None
_image_semaphore_loop: Optional[asyncio.AbstractEventLoop] = None


def _get_image_semaphore(limit: int = 3) -> asyncio.Semaphore:
    global _image_semaphore, _image_semaphore_loop

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.Semaphore(limit)

    if _image_semaphore is None or _image_semaphore_loop is not loop:
        _image_semaphore = asyncio.Semaphore(limit)
        _image_semaphore_loop = loop

    return _image_semaphore


async def _http_get_bytes(
    url: str,
    *,
    timeout: float = REQUEST_TIMEOUT,
    attempts: int = 3,
) -> tuple[bytes, str]:
    """تحميل صورة مع retry والتحقق من المحتوى."""
    last_error: Optional[Exception] = None

    for attempt in range(1, attempts + 1):
        try:
            async with _client_context() as client:
                response = await client.get(
                    url,
                    headers=HEADERS,
                    timeout=timeout,
                )

            if response.status_code in RETRYABLE_STATUS_CODES:
                if attempt < attempts:
                    retry_after = parse_retry_after(
                        response.headers.get("Retry-After")
                    )
                    await _sleep_backoff(attempt, retry_after)
                    continue
                raise ChapterImageError(
                    "تعذر تحميل الصورة بسبب خطأ مؤقت من الخادم."
                )

            if response.status_code != 200:
                raise ChapterImageError(
                    f"فشل تحميل الصورة. رمز: {response.status_code}"
                )

            content_type = (
                response.headers.get("content-type", "application/octet-stream")
                .split(";")[0]
                .strip()
                .lower()
            )

            content = response.content

            if not content:
                raise ChapterImageError("الصورة المسترجعة فارغة.")

            if len(content) <= 1024:
                raise ChapterImageError("حجم الصورة غير صالح (< 1KB).")

            if not content_type.startswith("image/"):
                raise ChapterImageError("الملف المسترجع ليس صورة صالحة.")

            return content, content_type

        except ChapterImageError:
            raise

        except (httpx.TimeoutException, httpx.RequestError) as exc:
            last_error = exc
            if attempt < attempts:
                await _sleep_backoff(attempt)
                continue

        except Exception as exc:
            last_error = exc
            if attempt < attempts:
                await _sleep_backoff(attempt)
                continue

    logger.error(
        "فشل تحميل الصورة بعد %s محاولات: %s", attempts, last_error
    )
    raise ChapterImageError("تعذر الاتصال بخادم الصور بعد عدة محاولات.")


async def _fetch_chapter_external_url(
    client: httpx.AsyncClient,
    chapter_id: str,
) -> Optional[str]:
    """فحص وجود externalUrl في الفصل. يعيد None عند 404."""
    try:
        payload = await _mangadex_get(client, f"/chapter/{chapter_id}")
    except HttpFailure as exc:
        if exc.status_code == 404:
            return None
        raise

    data = payload.get("data")
    if not isinstance(data, dict):
        return None

    return _chapter_external_url(data)


async def fetch_chapter_images(
    chapter_id: str,
    quality: str = "data",
    chapter_data: Optional[dict] = None,
) -> dict:
    """
    جلب روابط صور الفصل من MangaDex@Home.

    - quality: data (أصلية) | data-saver (مضغوطة).
    - chapter_data: إن مُرّر، يُستخدم بدل طلب /chapter/{id}.
    """
    chapter_id = str(chapter_id).strip()
    if not chapter_id:
        raise ChapterImageError("معرّف الفصل غير صالح.")

    if quality not in {"data", "data-saver"}:
        raise ChapterImageError(
            "الجودة غير مدعومة. استعمل data أو data-saver."
        )

    cache_key = f"{chapter_id}:{quality}"
    cached = await _image_cache.get(cache_key)
    if cached is not None:
        return cached

    async with _client_context() as client:
        # 1) هل الفصل خارجي؟
        if chapter_data is not None:
            external_url = _chapter_external_url(chapter_data)
        else:
            external_url = await _fetch_chapter_external_url(
                client, chapter_id
            )

        if external_url:
            result = {
                "chapter_id": chapter_id,
                "quality": quality,
                "external_url": external_url,
                "base_url": None,
                "hash": None,
                "pages": [],
                "total_pages": 0,
                "is_translatable": False,
                "message": (
                    "هذا الفصل متوفر عبر مصدر خارجي ولا يحتوي "
                    "على صفحات MangaDex مستضافة."
                ),
            }
            await _image_cache.set(cache_key, result)
            return result

        # 2) at-home/server
        try:
            payload = await _mangadex_get(
                client,
                f"/at-home/server/{chapter_id}",
            )
        except HttpFailure as exc:
            raise ChapterImageError(
                f"تعذر الحصول على خادم صور الفصل: {exc}"
            ) from exc

    if not isinstance(payload, dict):
        raise ChapterImageError("استجابة خادم الصور غير صالحة.")

    base_url = payload.get("baseUrl")
    chapter_info = payload.get("chapter")

    if not isinstance(base_url, str) or not base_url:
        raise ChapterImageError("عنوان خادم الصور غير موجود.")

    if not isinstance(chapter_info, dict):
        raise ChapterImageError("بيانات صور الفصل غير موجودة.")

    image_hash = chapter_info.get("hash")
    if not isinstance(image_hash, str) or not image_hash:
        raise ChapterImageError("معرّف مجموعة الصور غير موجود.")

    if quality == "data":
        filenames = chapter_info.get("data", [])
    else:
        filenames = chapter_info.get("dataSaver", [])

    if not isinstance(filenames, list):
        raise ChapterImageError("قائمة صفحات الفصل غير صالحة.")

    pages: list[dict] = []
    for index, filename in enumerate(filenames):
        if not isinstance(filename, str) or not filename:
            continue
        image_url = (
            f"{base_url.rstrip('/')}/{quality}/{image_hash}/{filename}"
        )
        pages.append({
            "page_index": index,
            "filename": filename,
            "url": image_url,
        })

    result = {
        "chapter_id": chapter_id,
        "quality": quality,
        "external_url": None,
        "base_url": base_url,
        "hash": image_hash,
        "pages": pages,
        "total_pages": len(pages),
        "is_translatable": len(pages) > 0,
        "message": (
            "تم العثور على صفحات الفصل."
            if pages
            else "لم يعثر MangaDex على صور لهذا الفصل."
        ),
    }

    # cache 15 دقيقة لأن baseUrl ينتهي
    await _image_cache.set(cache_key, result)
    return result


async def fetch_page_image(
    chapter_id: str,
    page_index: int,
    retry: int = 3,
    quality: str = "data",
) -> tuple[bytes, str]:
    """تحميل صفحة واحدة كـ bytes + content-type."""
    if page_index < 0:
        raise ChapterImageError("رقم الصفحة يجب أن يكون >= 0.")

    if retry < 1:
        raise ChapterImageError("عدد المحاولات يجب أن يكون >= 1.")

    chapter_images = await fetch_chapter_images(
        chapter_id, quality=quality
    )

    if chapter_images.get("external_url"):
        raise ChapterImageError(
            "هذا الفصل موجود في موقع خارجي ولا يمكن تحميل صوره."
        )

    pages = chapter_images.get("pages", [])
    if page_index >= len(pages):
        raise ChapterImageError("رقم الصفحة غير موجود في هذا الفصل.")

    page = pages[page_index]
    image_url = page.get("url")

    if not isinstance(image_url, str) or not image_url:
        raise ChapterImageError("رابط الصفحة غير صالح.")

    async with _get_image_semaphore():
        return await _http_get_bytes(image_url, attempts=retry)


# ==================================================
# IMAGE ROUTES
# ==================================================

@router.get("/manga/{manga_id}/chapters/{chapter_id}/images")
async def get_chapter_images_route(
    manga_id: str,
    chapter_id: str,
    quality: str = Query(default="data", pattern="^(data|data-saver)$"),
):
    """إرجاع قائمة روابط صور الفصل."""
    try:
        result = await fetch_chapter_images(chapter_id, quality=quality)
        result["manga_id"] = manga_id
        return result

    except ChapterImageError as exc:
        raise _image_error(str(exc), 404) from exc

    except HTTPException:
        raise

    except Exception as exc:
        logger.exception("خطأ أثناء جلب صور الفصل %s", chapter_id)
        raise _image_error("حدث خطأ أثناء جلب صور الفصل.", 502) from exc


@router.get("/manga/{manga_id}/chapters/{chapter_id}/pages/{page_index}")
async def get_chapter_page_image_route(
    manga_id: str,
    chapter_id: str,
    page_index: int,
    quality: str = Query(default="data", pattern="^(data|data-saver)$"),
) -> Response:
    """إرجاع صورة صفحة محددة مباشرة."""
    try:
        image_bytes, content_type = await fetch_page_image(
            chapter_id,
            page_index,
            retry=3,
            quality=quality,
        )

        return Response(
            content=image_bytes,
            media_type=content_type,
            headers={
                "Cache-Control": "public, max-age=900",
                "X-Chapter-ID": chapter_id,
                "X-Page-Index": str(page_index),
            },
        )

    except ChapterImageError as exc:
        raise _image_error(str(exc), 404) from exc

    except HTTPException:
        raise

    except Exception as exc:
        logger.exception(
            "خطأ أثناء تحميل صفحة %s من الفصل %s",
            page_index, chapter_id,
        )
        raise _image_error("حدث خطأ أثناء تحميل صورة الصفحة.", 502) from exc


# ============ END OF PART 3 ============
# ============ PART 4: TRANSLATION PIPELINE + JOB MANAGEMENT ============

# ==================================================
# DATA MODELS
# ==================================================

@dataclass
class TextBlock:
    """كتلة نصية مستخرجة من صورة."""
    bbox: tuple[int, int, int, int]
    text: str
    confidence: float
    reading_order: int


@dataclass
class TranslatedTextBlock:
    """كتلة نصية بعد الترجمة."""
    bbox: tuple[int, int, int, int]
    original_text: str
    translated_text: str
    confidence: float
    reading_order: int


@dataclass
class TranslatedPage:
    """نتيجة معالجة صفحة واحدة."""
    page_index: int
    image_url: Optional[str] = None
    blocks: list[TranslatedTextBlock] = field(default_factory=list)
    rendered_image: Optional[bytes] = None
    content_type: str = "image/png"


@dataclass
class TranslationResult:
    """النتيجة النهائية لترجمة الفصل."""
    chapter_id: str
    target_lang: str
    total_pages: int
    translated_pages: list[TranslatedPage] = field(default_factory=list)
    status: str = "completed"


# ==================================================
# ENGINE INTERFACES (Abstract)
# ==================================================

class OCREngine(ABC):
    @abstractmethod
    async def extract_text(self, image: bytes) -> list[TextBlock]:
        raise NotImplementedError("محرك OCR غير مربوط.")


class TranslationEngine(ABC):
    @abstractmethod
    async def translate_text(
        self, text: str, target_lang: str = "ar"
    ) -> str:
        raise NotImplementedError("محرك الترجمة غير مربوط.")


class InpaintingEngine(ABC):
    @abstractmethod
    async def remove_text(
        self, image: bytes, blocks: list[TextBlock]
    ) -> bytes:
        raise NotImplementedError("محرك Inpainting غير مربوط.")


class RenderingEngine(ABC):
    @abstractmethod
    async def render_page(
        self,
        image: bytes,
        blocks: list[TranslatedTextBlock],
        target_lang: str = "ar",
        rtl: bool = True,
    ) -> bytes:
        raise NotImplementedError("محرك Rendering غير مربوط.")


# ==================================================
# PLACEHOLDER ENGINES
# ==================================================

class PlaceholderOCREngine(OCREngine):
    async def extract_text(self, image: bytes) -> list[TextBlock]:
        raise NotImplementedError(
            "استخراج النصوص غير متاح: لم يتم ربط محرك OCR."
        )


class PlaceholderTranslationEngine(TranslationEngine):
    async def translate_text(
        self, text: str, target_lang: str = "ar"
    ) -> str:
        raise NotImplementedError(
            "الترجمة غير متاحة: لم يتم ربط محرك الترجمة."
        )


class PlaceholderInpaintingEngine(InpaintingEngine):
    async def remove_text(
        self, image: bytes, blocks: list[TextBlock]
    ) -> bytes:
        raise NotImplementedError(
            "إزالة النص الأصلي غير متاحة: لم يتم ربط محرك Inpainting."
        )


class PlaceholderRenderingEngine(RenderingEngine):
    async def render_page(
        self,
        image: bytes,
        blocks: list[TranslatedTextBlock],
        target_lang: str = "ar",
        rtl: bool = True,
    ) -> bytes:
        raise NotImplementedError(
            "رسم النص المترجم غير متاح: لم يتم ربط محرك Rendering."
        )


# ==================================================
# ENGINE REGISTRY
# ==================================================

ocr_engine: OCREngine = PlaceholderOCREngine()
translation_engine: TranslationEngine = PlaceholderTranslationEngine()
inpainting_engine: InpaintingEngine = PlaceholderInpaintingEngine()
rendering_engine: RenderingEngine = PlaceholderRenderingEngine()


def configure_translation_engines(
    *,
    ocr: Optional[OCREngine] = None,
    translator: Optional[TranslationEngine] = None,
    inpainting: Optional[InpaintingEngine] = None,
    renderer: Optional[RenderingEngine] = None,
) -> None:
    """
    استبدال المحركات الافتراضية بمحركات فعلية.

    الاستخدام:
        configure_translation_engines(ocr=MangaOCR(), translator=OllamaTranslator())
    """
    global ocr_engine, translation_engine, inpainting_engine, rendering_engine

    if ocr is not None:
        ocr_engine = ocr
    if translator is not None:
        translation_engine = translator
    if inpainting is not None:
        inpainting_engine = inpainting
    if renderer is not None:
        rendering_engine = renderer


# ==================================================
# JOB STORAGE (in-memory — Part 5 يضيف SQLite)
# ==================================================

_translation_jobs: dict[str, dict] = {}
_translation_jobs_lock = asyncio.Lock()

_translation_results: dict[str, TranslationResult] = {}
_translation_results_lock = asyncio.Lock()

# مهام خلفية — يجب الاحتفاظ بمرجع لها لتفادي GC
_background_tasks: set[asyncio.Task] = set()


def _spawn_background(coro) -> asyncio.Task:
    """تشغيل coroutine في الخلفية مع حفظ مرجع."""
    task = asyncio.create_task(coro)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


# ---- Stubs (Part 5 يعيد تعريفها بـ SQLite) ----

async def _save_translation_job(job: dict) -> None:
    """Placeholder — Part 5 يستبدلها بحفظ SQLite."""
    return None


async def _load_translation_job(job_id: str) -> Optional[dict]:
    """Placeholder — Part 5 يستبدلها بقراءة SQLite."""
    return None


# ---- Core job functions ----

async def _new_translation_job(
    manga_id: str,
    chapter_id: str,
    target_lang: str,
) -> dict:
    """إنشاء مهمة ترجمة جديدة وحفظها في الذاكرة + (SQLite عبر stub)."""
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    job = {
        "job_id": job_id,
        "manga_id": manga_id,
        "chapter_id": chapter_id,
        "target_lang": target_lang,
        "status": "pending",
        "progress": 0,
        "total_pages": 0,
        "processed_pages": 0,
        "current_page": None,
        "error": None,
        "created_at": now,
        "updated_at": now,
    }

    async with _translation_jobs_lock:
        _translation_jobs[job_id] = dict(job)

    await _save_translation_job(job)
    return dict(job)


async def _update_translation_job(
    job_id: str,
    **updates: Any,
) -> Optional[dict]:
    """تحديث مهمة في الذاكرة + (SQLite عبر stub)."""
    async with _translation_jobs_lock:
        job = _translation_jobs.get(job_id)

        if job is None:
            # محاولة التحميل من التخزين الدائم (Part 5)
            job = await _load_translation_job(job_id)
            if job is None:
                return None

        job.update(updates)
        job["updated_at"] = datetime.now(timezone.utc).isoformat()

        _translation_jobs[job_id] = dict(job)
        snapshot = dict(job)

    await _save_translation_job(snapshot)
    return snapshot


async def get_translation_job(job_id: str) -> Optional[dict]:
    """جلب حالة مهمة."""
    async with _translation_jobs_lock:
        job = _translation_jobs.get(job_id)
        if job is not None:
            return dict(job)

    # محاولة من التخزين الدائم
    loaded = await _load_translation_job(job_id)
    if loaded is None:
        return None

    async with _translation_jobs_lock:
        _translation_jobs[job_id] = dict(loaded)

    return loaded


# ==================================================
# PAGE TRANSLATION
# ==================================================

async def _translate_single_page(
    chapter_id: str,
    page_index: int,
    target_lang: str,
    quality: str = "data",
) -> TranslatedPage:
    """
    معالجة صفحة واحدة:
    تحميل ← OCR ← ترجمة ← مسح ← رسم.
    """
    image_bytes, content_type = await fetch_page_image(
        chapter_id=chapter_id,
        page_index=page_index,
        retry=3,
        quality=quality,
    )

    extracted_blocks = await ocr_engine.extract_text(image_bytes)
    extracted_blocks.sort(key=lambda b: b.reading_order)

    translated_blocks: list[TranslatedTextBlock] = []
    for block in extracted_blocks:
        if not block.text.strip():
            continue

        translated_text = await translation_engine.translate_text(
            block.text, target_lang=target_lang
        )

        translated_blocks.append(
            TranslatedTextBlock(
                bbox=block.bbox,
                original_text=block.text,
                translated_text=translated_text,
                confidence=block.confidence,
                reading_order=block.reading_order,
            )
        )

    cleaned_image = await inpainting_engine.remove_text(
        image_bytes, extracted_blocks
    )

    is_rtl = target_lang.lower() in {"ar", "he", "fa", "ur"}

    rendered_image = await rendering_engine.render_page(
        cleaned_image,
        translated_blocks,
        target_lang=target_lang,
        rtl=is_rtl,
    )

    return TranslatedPage(
        page_index=page_index,
        blocks=translated_blocks,
        rendered_image=rendered_image,
        content_type=content_type,
    )


# ==================================================
# CHAPTER TRANSLATION
# ==================================================

async def translate_chapter(
    chapter_id: str,
    target_lang: str = "ar",
    *,
    job_id: Optional[str] = None,
    quality: str = "data",
    max_concurrent_pages: int = 3,
) -> TranslationResult:
    """
    ترجمة كل صفحات الفصل.

    - يُعالج الصفحات بالتوازي مع حد أقصى.
    - عند فشل صفحة: يكمل الباقي ثم يرفع الخطأ.
    - يحفظ النتيجة في _translation_results.
    """
    chapter_id = str(chapter_id).strip()
    target_lang = str(target_lang).strip().lower()

    if not chapter_id:
        raise ValueError("معرّف الفصل غير صالح.")
    if not target_lang:
        raise ValueError("لغة الترجمة غير محددة.")
    if quality not in {"data", "data-saver"}:
        raise ValueError("جودة الصور غير مدعومة.")

    images_data = await fetch_chapter_images(chapter_id, quality=quality)

    if images_data.get("external_url"):
        raise ValueError(
            "لا يمكن ترجمة هذا الفصل لأنه متوفر عبر مصدر خارجي."
        )

    pages = images_data.get("pages", [])
    if not pages:
        raise ValueError("لا توجد صفحات صور متاحة لهذا الفصل.")

    total_pages = len(pages)

    if job_id:
        await _update_translation_job(
            job_id,
            status="processing",
            progress=0,
            total_pages=total_pages,
            processed_pages=0,
            current_page=None,
            error=None,
        )

    semaphore = asyncio.Semaphore(max_concurrent_pages)
    counter_lock = asyncio.Lock()
    completed = 0

    async def process_page(page_idx: int):
        nonlocal completed

        async with semaphore:
            if job_id:
                await _update_translation_job(
                    job_id, current_page=page_idx
                )

            try:
                result = await _translate_single_page(
                    chapter_id=chapter_id,
                    page_index=page_idx,
                    target_lang=target_lang,
                    quality=quality,
                )
            except Exception as exc:
                result = exc

            async with counter_lock:
                completed += 1
                current_completed = completed

            if job_id:
                progress = int(current_completed / total_pages * 100)
                await _update_translation_job(
                    job_id,
                    progress=progress,
                    processed_pages=current_completed,
                )

            return result

    tasks = [process_page(i) for i in range(total_pages)]
    results = await asyncio.gather(*tasks)

    pages_ok = [r for r in results if isinstance(r, TranslatedPage)]
    errors = [r for r in results if isinstance(r, Exception)]

    if errors:
        if job_id:
            await _update_translation_job(
                job_id,
                status="failed",
                error=(
                    f"فشلت {len(errors)} صفحة من {total_pages}. "
                    f"أول خطأ: {errors[0]}"
                ),
            )
        raise errors[0]

    pages_ok.sort(key=lambda p: p.page_index)

    result = TranslationResult(
        chapter_id=chapter_id,
        target_lang=target_lang,
        total_pages=total_pages,
        translated_pages=pages_ok,
        status="completed",
    )

    async with _translation_results_lock:
        _translation_results[f"{chapter_id}:{target_lang}"] = result

    if job_id:
        await _update_translation_job(
            job_id,
            status="completed",
            progress=100,
            processed_pages=total_pages,
            current_page=None,
            error=None,
        )

    return result


# ==================================================
# BACKGROUND JOB RUNNER
# ==================================================

async def _run_translation_job(
    job_id: str,
    manga_id: str,
    chapter_id: str,
    target_lang: str,
) -> None:
    """تشغيل الترجمة في الخلفية مع التقاط الأخطاء."""
    try:
        await _update_translation_job(
            job_id, status="processing", progress=0, error=None
        )

        await translate_chapter(
            chapter_id=chapter_id,
            target_lang=target_lang,
            job_id=job_id,
        )

    except NotImplementedError as exc:
        logger.warning(
            "محرك ترجمة غير مربوط للمهمة %s: %s", job_id, exc
        )
        await _update_translation_job(
            job_id, status="failed", error=str(exc)
        )

    except Exception as exc:
        logger.exception("فشلت مهمة الترجمة %s", job_id)
        await _update_translation_job(
            job_id, status="failed", error=str(exc)
        )


# ==================================================
# ROUTES
# ==================================================

@router.post("/manga/{manga_id}/chapters/{chapter_id}/translate")
async def start_chapter_translation_route(
    manga_id: str,
    chapter_id: str,
    target_lang: str = Query(default="ar"),
) -> dict:
    """
    بدء ترجمة فصل في الخلفية.

    - يتحقق من توفر الصور أولاً.
    - ينشئ job_id.
    - يعيد الاستجابة فوراً.
    - يتابع الترجمة في الخلفية.
    """
    target_lang = target_lang.strip().lower()

    if not target_lang:
        raise HTTPException(400, "لغة الترجمة غير صالحة.")

    if not _valid_language_code(target_lang):
        raise HTTPException(400, "رمز اللغة غير صالح.")

    try:
        images_data = await fetch_chapter_images(
            chapter_id, quality="data"
        )

        if images_data.get("external_url"):
            raise HTTPException(
                422,
                "هذا الفصل متوفر في موقع خارجي ولا يمكن ترجمته من صور MangaDex.",
            )

        total_pages = int(images_data.get("total_pages", 0))

        if total_pages <= 0:
            raise HTTPException(
                404, "لا توجد صور متاحة لهذا الفصل."
            )

    except ChapterImageError as exc:
        raise HTTPException(404, str(exc)) from exc

    except HTTPException:
        raise

    except Exception as exc:
        logger.exception(
            "تعذر التحقق من صور الفصل %s", chapter_id
        )
        raise HTTPException(
            502, f"تعذر التحقق من صور الفصل: {exc}"
        ) from exc

    # إنشاء المهمة
    job = await _new_translation_job(
        manga_id=manga_id,
        chapter_id=chapter_id,
        target_lang=target_lang,
    )

    await _update_translation_job(
        job["job_id"],
        total_pages=total_pages,
        status="pending",
    )

    # تشغيل في الخلفية
    _spawn_background(
        _run_translation_job(
            job_id=job["job_id"],
            manga_id=manga_id,
            chapter_id=chapter_id,
            target_lang=target_lang,
        )
    )

    return {
        "message": "تم إنشاء مهمة الترجمة.",
        "job_id": job["job_id"],
        "manga_id": manga_id,
        "chapter_id": chapter_id,
        "target_lang": target_lang,
        "status": "pending",
        "progress": 0,
        "total_pages": total_pages,
    }


@router.get("/jobs/{job_id}/progress")
async def get_job_progress_route(job_id: str) -> dict:
    """حالة مختصرة للمهمة (للتقدم)."""
    job = await get_translation_job(job_id)

    if job is None:
        raise HTTPException(404, "مهمة الترجمة غير موجودة.")

    return {
        "job_id": job["job_id"],
        "status": job.get("status"),
        "progress": int(job.get("progress", 0)),
        "processed_pages": int(job.get("processed_pages", 0)),
        "total_pages": int(job.get("total_pages", 0)),
        "current_page": job.get("current_page"),
        "error": job.get("error"),
        "updated_at": job.get("updated_at"),
    }


@router.get("/manga/{manga_id}/chapters/{chapter_id}/translated")
async def get_translated_chapter_route(
    manga_id: str,
    chapter_id: str,
    target_lang: str = Query(default="ar"),
) -> dict:
    """
    إرجاع الفصل المترجم إن كان جاهزاً (من الذاكرة).

    ملاحظة: الصور المترجمة في الذاكرة فقط حالياً.
    حفظها على القرص سيكون إضافة مستقبلية.
    """
    lang = str(target_lang).strip().lower()
    key = f"{chapter_id}:{lang}"

    async with _translation_results_lock:
        result = _translation_results.get(key)

    if result is None:
        raise HTTPException(
            404,
            "لا يوجد فصل مترجم جاهز. "
            "ابدأ الترجمة عبر POST .../translate أولاً، "
            "ثم تحقق من حالة المهمة عبر /jobs/{job_id}/progress.",
        )

    return {
        "manga_id": manga_id,
        "chapter_id": chapter_id,
        "target_lang": lang,
        "status": result.status,
        "total_pages": result.total_pages,
        "pages": [
            {
                "page_index": p.page_index,
                "blocks_count": len(p.blocks),
                "content_type": p.content_type,
                "has_rendered_image": p.rendered_image is not None,
            }
            for p in result.translated_pages
        ],
    }


# ============ END OF PART 4 ============
# ============ PART 5: SQLITE PERSISTENCE + CLEANUP + HEALTH ============

# ==================================================
# DATABASE CONFIGURATION
# ==================================================

DATABASE_PATH = os.getenv(
    "CHAPTERS_DATABASE_PATH",
    str(Path(__file__).resolve().parent / "data" / "chapters.sqlite3"),
)

DATABASE_TIMEOUT = 30

_database_initialized = False
_database_lock = asyncio.Lock()


def _database_connection() -> sqlite3.Connection:
    """إنشاء اتصال SQLite مع PRAGMA مناسبة."""
    db_file = Path(DATABASE_PATH)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(str(db_file), timeout=DATABASE_TIMEOUT)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def _initialize_database_sync() -> None:
    """إنشاء الجداول."""
    connection = _database_connection()
    try:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS chapters_cache (
                cache_key TEXT PRIMARY KEY,
                manga_id TEXT NOT NULL,
                language TEXT,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_chapters_cache_manga
                ON chapters_cache(manga_id);

            CREATE TABLE IF NOT EXISTS translation_progress (
                job_id TEXT PRIMARY KEY,
                manga_id TEXT NOT NULL,
                chapter_id TEXT NOT NULL,
                target_lang TEXT NOT NULL,
                status TEXT NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                total_pages INTEGER NOT NULL DEFAULT 0,
                processed_pages INTEGER NOT NULL DEFAULT 0,
                current_page INTEGER,
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_translation_progress_chapter
                ON translation_progress(chapter_id);

            CREATE TABLE IF NOT EXISTS translation_jobs (
                job_id TEXT PRIMARY KEY,
                manga_id TEXT NOT NULL,
                chapter_id TEXT NOT NULL,
                target_lang TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_translation_jobs_status
                ON translation_jobs(status);
            """
        )
        connection.commit()
    finally:
        connection.close()


async def init_db() -> None:
    """تهيئة قاعدة البيانات (idempotent)."""
    global _database_initialized

    async with _database_lock:
        if _database_initialized:
            return
        await asyncio.to_thread(_initialize_database_sync)
        _database_initialized = True
        logger.info("تم تهيئة قاعدة بيانات الفصول والترجمة.")


async def close_db() -> None:
    """إعادة تعيين الحالة (لا اتصالات دائمة)."""
    global _database_initialized
    async with _database_lock:
        _database_initialized = False


# ==================================================
# DATABASE EXECUTION HELPERS
# ==================================================

def _execute_database_sync(
    query: str,
    parameters: tuple = (),
    *,
    fetch: str = "none",
) -> Any:
    """تنفيذ استعلام متزامن (يُستدعى داخل to_thread)."""
    connection = _database_connection()
    try:
        cursor = connection.execute(query, parameters)

        if fetch == "one":
            row = cursor.fetchone()
            return dict(row) if row else None

        if fetch == "all":
            return [dict(row) for row in cursor.fetchall()]

        connection.commit()
        return cursor.rowcount

    finally:
        connection.close()


async def _database_execute(
    query: str,
    parameters: tuple = (),
    *,
    fetch: str = "none",
) -> Any:
    """تنفيذ استعلام غير متزامن (لا يحجب event loop)."""
    await init_db()
    return await asyncio.to_thread(
        _execute_database_sync,
        query,
        parameters,
        fetch=fetch,
    )


# ==================================================
# OVERRIDE: دوال الحفظ/التحميل من Part 4
# ==================================================

def _save_job_sync(job: dict) -> None:
    """حفظ المهمة في جدولي translation_jobs + translation_progress."""
    connection = _database_connection()
    try:
        payload = json.dumps(job, ensure_ascii=False, default=str)

        connection.execute(
            """
            INSERT INTO translation_jobs (
                job_id, manga_id, chapter_id, target_lang,
                status, payload, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                status = excluded.status,
                payload = excluded.payload,
                updated_at = excluded.updated_at
            """,
            (
                job["job_id"],
                job["manga_id"],
                job["chapter_id"],
                job["target_lang"],
                job["status"],
                payload,
                job["created_at"],
                job["updated_at"],
            ),
        )

        connection.execute(
            """
            INSERT INTO translation_progress (
                job_id, manga_id, chapter_id, target_lang,
                status, progress, total_pages, processed_pages,
                current_page, error, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                status = excluded.status,
                progress = excluded.progress,
                total_pages = excluded.total_pages,
                processed_pages = excluded.processed_pages,
                current_page = excluded.current_page,
                error = excluded.error,
                updated_at = excluded.updated_at
            """,
            (
                job["job_id"],
                job["manga_id"],
                job["chapter_id"],
                job["target_lang"],
                job["status"],
                int(job.get("progress", 0) or 0),
                int(job.get("total_pages", 0) or 0),
                int(job.get("processed_pages", 0) or 0),
                job.get("current_page"),
                job.get("error"),
                job["created_at"],
                job["updated_at"],
            ),
        )

        connection.commit()
    finally:
        connection.close()


def _load_job_sync(job_id: str) -> Optional[dict]:
    """قراءة المهمة من قاعدة البيانات."""
    connection = _database_connection()
    try:
        row = connection.execute(
            "SELECT payload FROM translation_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()

        if row is None:
            return None
        return json.loads(row["payload"])
    finally:
        connection.close()


# ✅ OVERRIDE — تُستدعى تلقائياً من Part 4 بفضل Python
async def _save_translation_job(job: dict) -> None:
    """نسخة Part 5: حفظ دائم في SQLite."""
    await init_db()
    await asyncio.to_thread(_save_job_sync, dict(job))


async def _load_translation_job(job_id: str) -> Optional[dict]:
    """نسخة Part 5: قراءة من SQLite."""
    await init_db()
    return await asyncio.to_thread(_load_job_sync, job_id)


# ==================================================
# CHAPTERS CACHE (SQLite-backed)
# ==================================================

async def save_chapters_cache(
    cache_key: str,
    manga_id: str,
    language: str,
    payload: dict,
    ttl: int = 3600,
) -> None:
    """حفظ قائمة فصول في SQLite (اختياري، للتشغيل بين الجلسات)."""
    now = datetime.now(timezone.utc)
    expires_at = (now + timedelta(seconds=ttl)).isoformat()

    await _database_execute(
        """
        INSERT INTO chapters_cache (
            cache_key, manga_id, language, payload,
            created_at, expires_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET
            manga_id = excluded.manga_id,
            language = excluded.language,
            payload = excluded.payload,
            created_at = excluded.created_at,
            expires_at = excluded.expires_at
        """,
        (
            cache_key,
            manga_id,
            language,
            json.dumps(payload, ensure_ascii=False, default=str),
            now.isoformat(),
            expires_at,
        ),
    )


async def load_chapters_cache(cache_key: str) -> Optional[dict]:
    """استرجاع قائمة فصول من SQLite إن لم تكن منتهية."""
    row = await _database_execute(
        "SELECT payload, expires_at FROM chapters_cache WHERE cache_key = ?",
        (cache_key,),
        fetch="one",
    )

    if row is None:
        return None

    expires_at = row.get("expires_at")
    if expires_at:
        try:
            expiry = datetime.fromisoformat(expires_at)
            if expiry <= datetime.now(timezone.utc):
                await _database_execute(
                    "DELETE FROM chapters_cache WHERE cache_key = ?",
                    (cache_key,),
                )
                return None
        except ValueError:
            return None

    try:
        return json.loads(row["payload"])
    except (TypeError, json.JSONDecodeError):
        return None


# ==================================================
# CACHE CLEANUP
# ==================================================

async def cleanup_expired_database_cache() -> int:
    """حذف سجلات chapters_cache المنتهية."""
    now = datetime.now(timezone.utc).isoformat()
    deleted = await _database_execute(
        """
        DELETE FROM chapters_cache
        WHERE expires_at IS NOT NULL AND expires_at <= ?
        """,
        (now,),
    )
    return max(int(deleted or 0), 0)


async def cleanup_memory_caches() -> dict:
    """تنظيف الكاشات في الذاكرة باستخدام cleanup() الجديدة."""
    results = {}
    caches = {
        "feed": _feed_cache,
        "languages": _languages_cache,
        "resolve": _resolve_cache,
        "image": _image_cache,
    }

    for name, cache in caches.items():
        try:
            results[name] = await cache.cleanup()
        except Exception:
            logger.exception("فشل تنظيف كاش %s", name)
            results[name] = 0

    return results


_cleanup_task: Optional[asyncio.Task] = None


async def periodic_cache_cleanup(interval_seconds: int = 1800) -> None:
    """تنظيف دوري كل 30 دقيقة."""
    while True:
        try:
            await asyncio.sleep(interval_seconds)

            db_deleted = await cleanup_expired_database_cache()
            memory_results = await cleanup_memory_caches()

            logger.info(
                "تنظيف الكاش: db_deleted=%s, memory=%s",
                db_deleted, memory_results,
            )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception("خطأ في التنظيف الدوري")


def start_cleanup_task() -> Optional[asyncio.Task]:
    """بدء مهمة التنظيف (تُستدعى من main.py lifespan)."""
    global _cleanup_task
    if _cleanup_task is None or _cleanup_task.done():
        _cleanup_task = asyncio.create_task(periodic_cache_cleanup())
    return _cleanup_task


async def stop_cleanup_task() -> None:
    """إيقاف مهمة التنظيف."""
    global _cleanup_task
    if _cleanup_task is not None:
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except asyncio.CancelledError:
            pass
        _cleanup_task = None


@asynccontextmanager
async def chapters_lifespan():
    """
    دورة حياة النمط الحديث — استعملها في main.py:

        from backend.chapters import chapters_lifespan
        app = FastAPI(lifespan=chapters_lifespan)

    أو استدعِ init_db/start_cleanup_task يدوياً.
    """
    await init_db()
    start_cleanup_task()

    try:
        yield
    finally:
        await stop_cleanup_task()
        await close_db()


# ==================================================
# DIAGNOSTIC ROUTES
# ==================================================

@router.get("/database/health")
async def database_health_route() -> dict:
    """التحقق من حالة قاعدة البيانات."""
    try:
        row = await _database_execute(
            "SELECT sqlite_version() AS version",
            fetch="one",
        )

        return {
            "status": "ok",
            "database": "SQLite",
            "sqlite_version": row["version"],
            "database_path": DATABASE_PATH,
            "initialized": _database_initialized,
        }

    except Exception as exc:
        logger.exception("فشل فحص قاعدة البيانات.")
        raise HTTPException(
            503, "قاعدة البيانات غير متاحة."
        ) from exc


@router.get("/jobs/{job_id}")
async def get_translation_job_full_route(job_id: str) -> dict:
    """
    تفاصيل المهمة الكاملة (بما فيها created_at, updated_at).
    للنسخة المختصرة، استخدم /jobs/{job_id}/progress.
    """
    job = await get_translation_job(job_id)

    if job is None:
        raise HTTPException(404, "مهمة الترجمة غير موجودة.")

    return job


@router.get("/jobs")
async def list_translation_jobs_route(
    limit: int = Query(default=50, ge=1, le=200),
    status: Optional[str] = Query(default=None),
) -> dict:
    """قائمة المهام (مفيدة للتشخيص)."""
    if status:
        rows = await _database_execute(
            """
            SELECT payload FROM translation_jobs
            WHERE status = ?
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (status, limit),
            fetch="all",
        )
    else:
        rows = await _database_execute(
            """
            SELECT payload FROM translation_jobs
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
            fetch="all",
        )

    jobs = []
    for row in rows:
        try:
            jobs.append(json.loads(row["payload"]))
        except (TypeError, json.JSONDecodeError):
            continue

    return {"total": len(jobs), "jobs": jobs}


# ==================================================
# RETRY HELPER (اختياري — للاستخدام من كود خارجي)
# ==================================================

async def retry_async(
    operation: Callable[..., Any],
    *args: Any,
    attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
    **kwargs: Any,
) -> Any:
    """
    إعادة تنفيذ عملية غير متزامنة مع exponential backoff.
    """
    if attempts < 1:
        raise ValueError("attempts يجب أن يكون >= 1.")

    last_error: Optional[Exception] = None

    for attempt in range(attempts):
        try:
            result = operation(*args, **kwargs)
            if asyncio.iscoroutine(result):
                return await result
            return result

        except (httpx.TimeoutException, httpx.RequestError) as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                break
            delay = min(base_delay * (2 ** attempt), max_delay)
            await asyncio.sleep(delay)

    if last_error is not None:
        raise last_error
    raise RuntimeError("فشلت العملية بعد كل المحاولات.")

# ============ END OF PART 5 ============
# ============ TESTS: CHAPTERS ============

import asyncio
import json
import os

import pytest

from unittest.mock import AsyncMock, patch


# ==================================================
# TEST 1: Pagination > 500 chapters
# ==================================================

@pytest.mark.asyncio
async def test_pagination_beyond_500_chapters():
    """
    اختبار جلب أكثر من 500 فصل عبر pagination.

    ملاحظة: _mangadex_get الآن يعيد dict فقط (وليس tuple).
    """
    from backend import chapters

    first_page = [
        {
            "id": f"chap-{index}",
            "attributes": {
                "volume": "1",
                "chapter": str(index),
                "translatedLanguage": "en",
                "pages": 10,
            },
        }
        for index in range(500)
    ]

    second_page = [
        {
            "id": f"chap-{index}",
            "attributes": {
                "volume": "2",
                "chapter": str(index),
                "translatedLanguage": "en",
                "pages": 10,
            },
        }
        for index in range(500, 600)
    ]

    responses = [
        {"data": first_page, "total": 600},
        {"data": second_page, "total": 600},
    ]

    async def fake_mangadex_get(*args, **kwargs):
        return responses.pop(0)

    with patch.object(
        chapters,
        "_mangadex_get",
        new=AsyncMock(side_effect=fake_mangadex_get),
    ):
        result = await chapters.fetch_all_chapters(
            "test-manga-id",
            language="en",
            client=AsyncMock(),
        )

    assert len(result) == 600
    assert result[0]["attributes"]["chapter"] == "0"
    assert result[-1]["attributes"]["chapter"] == "599"


# ==================================================
# TEST 2: Deduplication picks the newest
# ==================================================

def test_deduplication_picks_one_per_chapter():
    """اختيار نسخة واحدة لكل فصل، وتفضيل الأحدث publishAt."""
    from backend import chapters

    records = [
        {
            "id": "chapter-older",
            "attributes": {
                "volume": "1",
                "chapter": "1",
                "translatedLanguage": "en",
                "pages": 10,
                "publishAt": "2026-01-01T00:00:00+00:00",
            },
        },
        {
            "id": "chapter-newer",
            "attributes": {
                "volume": "1",
                "chapter": "1",
                "translatedLanguage": "en",
                "pages": 10,
                "publishAt": "2026-02-01T00:00:00+00:00",
            },
        },
    ]

    result = chapters._deduplicate_chapters(records)

    assert len(result) == 1
    assert result[0]["id"] == "chapter-newer"


# ==================================================
# TEST 3: Sort order ascending
# ==================================================

def test_sort_order_ascending():
    """الترتيب تصاعدي حسب (volume ثم chapter)."""
    from backend import chapters

    records = [
        {"id": "3", "attributes": {"volume": "1", "chapter": "3", "translatedLanguage": "en"}},
        {"id": "1", "attributes": {"volume": "1", "chapter": "1", "translatedLanguage": "en"}},
        {"id": "2", "attributes": {"volume": "1", "chapter": "2", "translatedLanguage": "en"}},
    ]

    result = sorted(records, key=chapters._chapter_sort_key)
    numbers = [chapters._chapter_number(item) for item in result]

    # _chapter_number يعيد float وليس str
    assert numbers == [1.0, 2.0, 3.0]


# ==================================================
# TEST 4: externalUrl excluded from translation
# ==================================================

def test_external_url_excluded():
    """الفصول الخارجية تُوسم is_translatable=False."""
    from backend import chapters

    record = {
        "id": "external-chapter",
        "attributes": {
            "volume": "1",
            "chapter": "1",
            "translatedLanguage": "en",
            "pages": 0,
            "externalUrl": "https://example.com/chapter",
        },
    }

    result = chapters._serialize_chapter(record)

    assert result["is_translatable"] is False
    assert result["externalUrl"] == "https://example.com/chapter"


# ==================================================
# TEST 5: Image fetch retries on failure
# ==================================================

@pytest.mark.asyncio
async def test_images_fetch_retry_on_failure():
    """
    _http_get_bytes تعيد المحاولة عند فشل الشبكة.
    نُموّه _sleep_backoff لتفادي انتظار ~1.8 ثانية.
    """
    from backend import chapters

    attempts = 0

    async def fake_get(*args, **kwargs):
        nonlocal attempts
        attempts += 1

        if attempts < 3:
            raise chapters.httpx.ConnectError("temporary failure")

        class FakeResponse:
            status_code = 200
            content = b"x" * 2048
            headers = {"content-type": "image/jpeg"}

        return FakeResponse()

    class FakeClient:
        async def get(self, *args, **kwargs):
            return await fake_get(*args, **kwargs)

    class FakeContext:
        async def __aenter__(self):
            return FakeClient()

        async def __aexit__(self, *args):
            return None

    with (
        patch.object(chapters, "_client_context", new=lambda: FakeContext()),
        patch.object(chapters, "_sleep_backoff", new=AsyncMock(return_value=None)),
    ):
        content, content_type = await chapters._http_get_bytes(
            "https://example.com/image.jpg",
            attempts=3,
        )

    assert attempts == 3
    assert len(content) > 1024
    assert content_type == "image/jpeg"


# ==================================================
# TEST 6: Translate chapter flow (with mocks)
# ==================================================

@pytest.mark.asyncio
async def test_translate_chapter_flow():
    """ترجمة فصل كامل عبر محركات وهمية."""
    from backend import chapters

    fake_blocks = [
        chapters.TextBlock(
            bbox=(10, 10, 100, 50),
            text="Hello",
            confidence=0.99,
            reading_order=1,
        )
    ]

    fake_images_data = {
        "external_url": None,
        "pages": [{"page_index": 0, "url": "test", "filename": "p1.png"}],
        "total_pages": 1,
        "is_translatable": True,
    }

    with (
        patch.object(
            chapters,
            "fetch_chapter_images",
            new=AsyncMock(return_value=fake_images_data),
        ),
        patch.object(
            chapters,
            "fetch_page_image",
            new=AsyncMock(return_value=(b"x" * 2048, "image/jpeg")),
        ),
        patch.object(
            chapters.ocr_engine,
            "extract_text",
            new=AsyncMock(return_value=fake_blocks),
        ),
        patch.object(
            chapters.translation_engine,
            "translate_text",
            new=AsyncMock(return_value="مرحبا"),
        ),
        patch.object(
            chapters.inpainting_engine,
            "remove_text",
            new=AsyncMock(return_value=b"cleaned-image"),
        ),
        patch.object(
            chapters.rendering_engine,
            "render_page",
            new=AsyncMock(return_value=b"rendered-image"),
        ),
    ):
        result = await chapters.translate_chapter(
            "test-chapter", target_lang="ar"
        )

    assert result.status == "completed"
    assert result.total_pages == 1
    assert len(result.translated_pages) == 1
    assert result.translated_pages[0].blocks[0].translated_text == "مرحبا"


# ==================================================
# TEST 7: SQLite persistence
# ==================================================

@pytest.mark.asyncio
async def test_sqlite_progress_persistence(tmp_path):
    """المهام تُحفظ في SQLite وتُستَرجَع بعد مسح الذاكرة."""
    from backend import chapters

    old_path = chapters.DATABASE_PATH
    old_initialized = chapters._database_initialized

    chapters.DATABASE_PATH = str(tmp_path / "test.sqlite3")
    chapters._database_initialized = False

    try:
        await chapters.init_db()

        job = {
            "job_id": "test-job",
            "manga_id": "test-manga",
            "chapter_id": "test-chapter",
            "target_lang": "ar",
            "status": "processing",
            "progress": 40,
            "total_pages": 10,
            "processed_pages": 4,
            "current_page": 4,
            "error": None,
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:01:00+00:00",
        }

        await chapters._save_translation_job(job)

        # مسح الذاكرة لمحاكاة إعادة التشغيل
        async with chapters._translation_jobs_lock:
            chapters._translation_jobs.clear()

        loaded = await chapters.get_translation_job("test-job")

        assert loaded is not None
        assert loaded["progress"] == 40
        assert loaded["status"] == "processing"
        assert loaded["chapter_id"] == "test-chapter"

    finally:
        chapters.DATABASE_PATH = old_path
        chapters._database_initialized = old_initialized


# ============ END OF TESTS ============
