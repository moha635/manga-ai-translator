from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Manga AI Translator API",
    description="Backend for Arabic Manga and Manhwa platform",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def home():
    return {
        "message": "Manga AI Translator API is running",
        "status": "online"
    }


@app.get("/health")
def health():
    return {
        "status": "healthy"
    }


@app.get("/api/v1/catalog/search")
def search_catalog(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100)
):
    return {
        "query": q,
        "results": [],
        "total": 0,
        "message": "Catalog search integration is coming next"
    }
