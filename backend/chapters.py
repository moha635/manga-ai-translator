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


async def fetch_language(client, manga_id, code):
    async with semaphore:
        params = {
            "translatedLanguage[]": code,
            "limit": 1,
            "order[readableAt]": "desc",
        }

        try:
            response = await client.get(
                f"{BASE_URL}/manga/{manga_id}/feed",
                params=params,
                timeout=25,
            )

            if response.status_code != 200:
                logger.error(
                    "MangaDex error %s for %s: %s",
                    response.status_code,
                    code,
                    response.text[:500],
                )

                return {
                    "language_code": code,
                    "language_name": LANGUAGES[code],
                    "chapter_count": None,
                    "latest_chapter": None,
                    "error": f"HTTP {response.status_code}",
                }

            data = response.json()
            chapters = data.get("data", [])

            latest = None

            if chapters:
                chapter = chapters[0]
                attr = chapter.get("attributes", {})
                chapter_id = chapter.get("id")

                latest = {
                    "id": chapter_id,
                    "chapter": attr.get("chapter"),
                    "title": attr.get("title"),
                    "volume": attr.get("volume"),
                    "published_at": attr.get("readableAt"),
                    "url": f"https://mangadex.org/chapter/{chapter_id}",
                }

            return {
                "language_code": code,
                "language_name": LANGUAGES[code],
                "chapter_count": data.get("total", 0),
                "latest_chapter": latest,
            }

        except httpx.TimeoutException:
            logger.warning("Timeout for language %s", code)

            return {
                "language_code": code,
                "language_name": LANGUAGES[code],
                "chapter_count": None,
                "latest_chapter": None,
                "error": "Request timeout",
            }

        except Exception as error:
            logger.exception("Error for language %s", code)

            return {
                "language_code": code,
                "language_name": LANGUAGES[code],
                "chapter_count": None,
                "latest_chapter": None,
                "error": str(error),
            }


@router.get("/manga/{manga_id}/chapters")
async def get_manga_chapters(
    manga_id: str,
    languages: list[str] | None = Query(default=None),
):
    selected = languages if languages else list(LANGUAGES)

    if len(selected) > 15:
        raise HTTPException(
            status_code=400,
            detail="Maximum 15 languages",
        )

    unknown = [
        code for code in selected
        if code not in LANGUAGES
    ]

    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported languages: {unknown}",
        )

    async with httpx.AsyncClient(
        headers={
            "User-Agent": "MangaAITranslator/4.0",
            "Accept": "application/json",
        }
    ) as client:

        results = await asyncio.gather(
            *[
                fetch_language(client, manga_id, code)
                for code in selected
            ]
        )

    arabic = next(
        (
            item for item in results
            if item["language_code"] == "ar"
        ),
        None,
    )

    if arabic is None:
        arabic_status = "not_checked"
    elif arabic["chapter_count"] is None:
        arabic_status = "unknown"
    elif arabic["chapter_count"] > 0:
        arabic_status = "available"
    else:
        arabic_status = "not_available"

    available = sum(
        1 for item in results
        if item["chapter_count"] is not None
        and item["chapter_count"] > 0
    )

    return {
        "manga_id": manga_id,
        "source": "MangaDex",
        "arabic_status": arabic_status,
        "available_languages": available,
        "languages": results,
    }
