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
from fastapi.responses import HTMLResponse

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
    docs_url="/docs",
    redoc_url=None,  # Desactivar el ReDoc por defecto (CDN roto)
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


@app.get("/redoc", include_in_schema=False)
async def custom_redoc():
    """ReDoc con CDN fijado a versión estable (evita el bug de @next)."""
    return HTMLResponse(f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>{app.title} - ReDoc</title>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">
        <style>body {{ margin: 0; padding: 0; }}</style>
    </head>
    <body>
        <redoc spec-url='{app.openapi_url}'></redoc>
        <script src="https://cdn.redoc.ly/redoc/v2.1.5/bundles/redoc.standalone.js"></script>
    </body>
    </html>
    """)


@app.get("/")
async def root():
    return {
        "project": "CryptoLake",
        "docs": "/docs",
        "redoc": "/redoc",
        "health": "/api/v1/health",
    }