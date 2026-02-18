"""Health check endpoint."""

from fastapi import APIRouter

from src.serving.api.database import execute_query
from src.serving.api.models.schemas import HealthResponse

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Verifica que la API puede conectar con el Lakehouse."""
    try:
        tables = execute_query("SHOW TABLES IN cryptolake.gold")
        return HealthResponse(
            status="healthy",
            thrift_connected=True,
            tables_available=len(tables),
        )
    except Exception:
        return HealthResponse(
            status="degraded",
            thrift_connected=False,
            tables_available=0,
        )
