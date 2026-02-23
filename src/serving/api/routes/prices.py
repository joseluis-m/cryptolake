"""Price endpoints querying fact_market_daily."""

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from src.serving.api.database import execute_query
from src.serving.api.models.schemas import PriceResponse

router = APIRouter(tags=["Prices"])


@router.get("/prices/{coin_id}", response_model=list[PriceResponse])
async def get_prices(
    coin_id: str,
    start_date: Optional[date] = Query(
        default=None,
        description="Start date (default: 30 days ago)",
    ),
    end_date: Optional[date] = Query(
        default=None,
        description="End date (default: today)",
    ),
    limit: int = Query(default=100, le=1000),
):
    """Historical prices for a cryptocurrency.

    Includes computed metrics: moving averages, volatility,
    technical signals, and market sentiment.
    """
    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=30)

    try:
        rows = execute_query(f"""
            SELECT
                coin_id, price_date, price_usd,
                market_cap_usd, volume_24h_usd,
                price_change_pct_1d,
                moving_avg_7d, moving_avg_30d,
                volatility_7d,
                fear_greed_value, market_sentiment,
                ma30_signal
            FROM cryptolake.gold.fact_market_daily
            WHERE coin_id = '{coin_id}'
              AND price_date BETWEEN '{start_date}' AND '{end_date}'
            ORDER BY price_date DESC
            LIMIT {limit}
        """)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {e}")

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for {coin_id}",
        )
    return rows
