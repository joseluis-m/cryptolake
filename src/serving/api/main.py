"""
API REST de CryptoLake.

Endpoints:
- GET /api/v1/prices/{coin_id}           — Precios históricos
- GET /api/v1/analytics/market-overview   — Overview del mercado
- GET /api/v1/analytics/coins             — Lista de criptomonedas
- GET /api/v1/analytics/fear-greed        — Fear & Greed histórico
- GET /api/v1/health                      — Health check

Documentación automática:
- Swagger UI: http://localhost:8000/docs
- ReDoc:      http://localhost:8000/redoc
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.serving.api.routes import analytics, health, prices


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    print("🚀 CryptoLake API starting...")
    yield
    print("👋 CryptoLake API shutting down...")


app = FastAPI(
    title="CryptoLake API",
    description=(
        "Real-time crypto analytics powered by a Lakehouse architecture. "
        "Queries Apache Iceberg tables via Spark Thrift Server."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(prices.router, prefix="/api/v1")
app.include_router(analytics.router, prefix="/api/v1")
app.include_router(health.router, prefix="/api/v1")


@app.get("/")
async def root():
    return {
        "project": "CryptoLake",
        "docs": "/docs",
        "health": "/api/v1/health",
    }
