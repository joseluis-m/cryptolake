"""
Endpoints analíticos — market overview, fear & greed, coins.
"""

from fastapi import APIRouter, HTTPException, Query

from src.serving.api.database import execute_query
from src.serving.api.models.schemas import (
    CoinResponse,
    FearGreedResponse,
    MarketOverview,
)

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/market-overview", response_model=MarketOverview)
async def get_market_overview():
    """Resumen general del mercado crypto."""
    try:
        coins = execute_query("SELECT COUNT(*) AS cnt FROM cryptolake.gold.dim_coins")
        facts = execute_query("""
            SELECT COUNT(*) AS cnt,
                   MIN(price_date) AS min_d,
                   MAX(price_date) AS max_d
            FROM cryptolake.gold.fact_market_daily
        """)
        fg = execute_query("""
            SELECT fear_greed_value, classification
            FROM cryptolake.silver.fear_greed
            ORDER BY index_date DESC LIMIT 1
        """)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return MarketOverview(
        total_coins=coins[0]["cnt"] if coins else 0,
        date_range_start=facts[0]["min_d"] if facts else None,
        date_range_end=facts[0]["max_d"] if facts else None,
        total_fact_rows=facts[0]["cnt"] if facts else 0,
        latest_fear_greed=fg[0]["fear_greed_value"] if fg else None,
        latest_sentiment=fg[0]["classification"] if fg else None,
    )


@router.get("/coins", response_model=list[CoinResponse])
async def get_coins():
    """Lista todas las criptomonedas con estadísticas."""
    try:
        rows = execute_query("""
            SELECT coin_id, first_tracked_date, last_tracked_date,
                   total_days_tracked, all_time_low, all_time_high,
                   avg_price, avg_daily_volume, price_range_pct
            FROM cryptolake.gold.dim_coins
            ORDER BY avg_price DESC
        """)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return rows


@router.get("/fear-greed", response_model=list[FearGreedResponse])
async def get_fear_greed(
    limit: int = Query(default=30, le=365),
):
    """Histórico del Fear & Greed Index."""
    try:
        rows = execute_query(f"""
            SELECT index_date, fear_greed_value, classification
            FROM cryptolake.silver.fear_greed
            ORDER BY index_date DESC
            LIMIT {limit}
        """)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return rows
