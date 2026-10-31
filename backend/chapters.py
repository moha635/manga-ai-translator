import asyncio
import logging
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(
    prefix="/api/v1",
    tags=["Chapters"]
)

logger = logging.getLogger("manga-chapters")

MANGADEX_URL = "https://api.mangadex.org"

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


def extract_chapter_count(data):
    total = 0

    volumes = data.get("volumes", {})

    for volume in volumes.values():
        chapters = volume.get("chapters", {})
        total += len(chapters)

    return total


async def get_language_data(client, manga_id, language):
    try:
        response = await client.get(
            f"{MANGADEX_URL}/manga/{manga_id}/aggregate",
            params=[
                ("translatedLanguage[]", language)
            ]
        )

        response.raise_for_status()
        payload = response.json()

        if payload.get("result") != "ok":
            raise ValueError("Invalid MangaDex response")

        aggregate = payload.get("volumes", {})
        count = extract_chapter_count({
            "volumes": aggregate
        })

        latest_response = await client.get(
            f"{MANGADEX_URL}/manga/{manga_id}/feed",
            params=[
                ("translatedLanguage[]", language),
                ("limit", 1),
                ("order[publishAt]", "desc"),
                ("includeFuturePublishAt", "0")
            ]
        )

        latest_response.raise_for_status()

        chapters = latest_response.json().get("data", [])
        latest = None

        if chapters:
            item = chapters[0]
            attributes = item.get("attributes", {})

            latest = {
                "id": item.get("id"),
                "chapter": attributes.get("chapter"),
                "title": attributes.get("title"),
                "volume": attributes.get("volume"),
                "published_at": attributes.get("publishAt"),
                "url": (
                    "https://mangadex.org/chapter/"
                    + item.get("id", "")
                )
            }

        return {
            "language_code": language,
            "language_name": LANGUAGES.get(language, language),
            "chapter_count": count,
            "latest_chapter": latest
        }

    except Exception as error:
        logger.warning(
            "Language %s failed: %s",
            language,
            error
        )

        return {
            "language_code": language,
            "language_name": LANGUAGES.get(language, language),
            "chapter_count": None,
            "latest_chapter": None,
            "error": "Source temporarily unavailable"
        }


@router.get("/manga/{manga_id}/chapters")
async def get_manga_chapters(
    manga_id: str,
    languages: Optional[list[str]] = Query(default=None)
):
    selected = languages or list(LANGUAGES.keys())

    selected = list(dict.fromkeys(selected))

    if len(selected) > 15:
        raise HTTPException(
            status_code=400,
            detail="Maximum 15 languages per request"
        )

    for language in selected:
        if language not in LANGUAGES:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported language: {language}"
            )

    headers = {
        "User-Agent": "MangaAITranslator/1.0",
        "Accept": "application/json"
    }

    timeout = httpx.Timeout(30.0)

    async with httpx.AsyncClient(
        headers=headers,
        timeout=timeout
    ) as client:

        tasks = [
            get_language_data(client, manga_id, language)
            for language in selected
        ]

        results = await asyncio.gather(*tasks)

    available = [
        item for item in results
        if item and item.get("chapter_count") is not None
        and item.get("chapter_count", 0) > 0
    ]

    arabic = next(
        (
            item for item in results
            if item and item["language_code"] == "ar"
        ),
        None
    )

    if arabic is None or arabic.get("error"):
        arabic_status = "unknown"
    elif arabic.get("chapter_count", 0) == 0:
        arabic_status = "not_available_on_source"
    else:
        arabic_status = "available"

    return {
        "manga_id": manga_id,
        "source": "MangaDex",
        "arabic_status": arabic_status,
        "available_languages": len(available),
        "languages": results
    }
