from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router as api_router
from app.core.config import settings

app = FastAPI(
    title="Kanen Coffee Repair Intelligence API",
    version="0.1.0",
    description="Data + analytics service for Kanen Coffee operations"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

from app.api.sentiment_routes import router as sentiment_router
app.include_router(sentiment_router)


@app.get("/health", tags=["system"])
async def health_check() -> dict[str, str]:
    return {"status": "Johnny 5 is alive", "environment": settings.environment}
