import asyncio
import logging

import httpx
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/v1", tags=["Chapters"])

BASE_URL = "https://api.mangadex.org"

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

semaphore = asyncio.Semaphore(2)


async def fetch_language(client, manga_id, code, name):
    params = [
        ("manga[]", manga_id),
        ("translatedLanguage[]", code),
        ("limit", "1"),
        ("order[readableAt]", "desc"),
    ]

    async with semaphore:
        for attempt in range(3):
            try:
                response = await client.get(
                    f"{BASE_URL}/chapter",
                    params=params,
                    timeout=20,
                )

                if response.status_code == 429:
                    await asyncio.sleep(2 ** (attempt + 1))
                    continue

                response.raise_for_status()
                data = response.json()

                total = data.get("total", 0)
                chapters = data.get("data", [])

                latest = None

                if chapters:
                    chapter = chapters[0]
                    attributes = chapter.get("attributes", {})

                    latest = {
                        "id": chapter.get("id"),
                        "chapter": attributes.get("chapter"),
                        "title": attributes.get("title"),
                        "volume": attributes.get("volume"),
                        "published_at": attributes.get("readableAt"),
                        "url": (
                            "https://mangadex.org/chapter/"
                            + chapter.get("id", "")
                        ),
                    }

                return {
                    "language_code": code,
                    "language_name": name,
                    "chapter_count": total,
                    "latest_chapter": latest,
                }

            except Exception as error:
                logger.warning(
                    "Attempt %s failed for %s: %s",
                    attempt + 1,
                    code,
                    error,
                )

                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)

        return {
            "language_code": code,
            "language_name": name,
            "chapter_count": None,
            "latest_chapter": None,
            "error": "Source temporarily unavailable",
        }


@router.get("/manga/{manga_id}/chapters")
async def get_manga_chapters(
    manga_id: str,
    languages: list[str] | None = Query(default=None),
):
    selected = languages or list(LANGUAGES.keys())

    if len(selected) > 15:
        raise HTTPException(
            status_code=400,
            detail="Maximum 15 languages per request",
        )

    unknown = [code for code in selected if code not in LANGUAGES]

    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported languages: {unknown}",
        )

    async with httpx.AsyncClient(
        headers={"User-Agent": "MangaAITranslator/4.0"}
    ) as client:

        results = await asyncio.gather(
            *[
                fetch_language(
                    client,
                    manga_id,
                    code,
                    LANGUAGES[code],
                )
                for code in selected
            ]
        )

    arabic = next(
        (item for item in results if item["language_code"] == "ar"),
        None,
    )

    if arabic is None:
        arabic_status = "not_checked"
    elif arabic.get("chapter_count") is None:
        arabic_status = "unknown"
    elif arabic["chapter_count"] > 0:
        arabic_status = "available"
    else:
        arabic_status = "not_available"

    available = sum(
        1 for item in results
        if item.get("chapter_count", 0) > 0
    )

    return {
        "manga_id": manga_id,
        "source": "MangaDex",
        "arabic_status": arabic_status,
        "available_languages": available,
        "languages
