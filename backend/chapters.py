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

                if response.status_code == 
