"""
Pydantic response models aligned with the Gold star schema.

FastAPI uses these models to validate response data, generate
automatic OpenAPI documentation, and serialize types like date/float.
"""

from datetime import date
from typing import Optional

from pydantic import BaseModel


class PriceResponse(BaseModel):
    """A row from fact_market_daily."""

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
    """A row from dim_coins."""

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
    """Aggregated market summary."""

    total_coins: int
    date_range_start: Optional[date] = None
    date_range_end: Optional[date] = None
    total_fact_rows: int
    latest_fear_greed: Optional[int] = None
    latest_sentiment: Optional[str] = None


class FearGreedResponse(BaseModel):
    """A single Fear & Greed Index entry."""

    index_date: date
    fear_greed_value: int
    classification: str


class HealthResponse(BaseModel):
    """API health check response."""

    status: str
    thrift_connected: bool
    tables_available: int
