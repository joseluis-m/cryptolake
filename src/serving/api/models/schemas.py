"""
Modelos de respuesta Pydantic — alineados con el star schema Gold.

Estos modelos definen la estructura exacta de las respuestas JSON
de la API. FastAPI los usa para:
- Validar la forma de los datos antes de enviarlos
- Generar documentación OpenAPI automática
- Serializar correctamente tipos como date/float
"""

from datetime import date
from typing import Optional

from pydantic import BaseModel


class PriceResponse(BaseModel):
    """Una fila de fact_market_daily."""

    coin_id: str
    price_date: date
    price_usd: float
    market_cap_usd: Optional[float] = None
    volume_24h_usd: Optional[float] = None
    price_change_pct_1d: Optional[float] = None
    moving_avg_7d: Optional[float] = None
    moving_avg_30d: Optional[float] = None
    volatility_7d: Optional[float] = None
    fear_greed_value: Optional[int] = None
    market_sentiment: Optional[str] = None
    ma30_signal: Optional[str] = None


class CoinResponse(BaseModel):
    """Una fila de dim_coins."""

    coin_id: str
    first_tracked_date: Optional[date] = None
    last_tracked_date: Optional[date] = None
    total_days_tracked: Optional[int] = None
    all_time_low: Optional[float] = None
    all_time_high: Optional[float] = None
    avg_price: Optional[float] = None
    avg_daily_volume: Optional[float] = None
    price_range_pct: Optional[float] = None


class MarketOverview(BaseModel):
    """Resumen general del mercado."""

    total_coins: int
    date_range_start: Optional[date] = None
    date_range_end: Optional[date] = None
    total_fact_rows: int
    latest_fear_greed: Optional[int] = None
    latest_sentiment: Optional[str] = None


class FearGreedResponse(BaseModel):
    """Un dato del Fear & Greed Index."""

    index_date: date
    fear_greed_value: int
    classification: str


class HealthResponse(BaseModel):
    """Health check de la API."""

    status: str
    thrift_connected: bool
    tables_available: int
