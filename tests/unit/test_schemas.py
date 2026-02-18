"""Tests unitarios para los schemas de la API."""

from datetime import date

from src.serving.api.models.schemas import (
    CoinResponse,
    FearGreedResponse,
    HealthResponse,
    MarketOverview,
    PriceResponse,
)


def test_price_response():
    """PriceResponse se instancia correctamente con datos mínimos."""
    p = PriceResponse(
        coin_id="bitcoin",
        price_date=date(2025, 1, 15),
        price_usd=95000.0,
    )
    assert p.coin_id == "bitcoin"
    assert p.price_usd == 95000.0
    assert p.moving_avg_7d is None  # Optional


def test_coin_response():
    """CoinResponse se instancia correctamente."""
    c = CoinResponse(
        coin_id="ethereum",
        all_time_high=4000.0,
        avg_price=3200.0,
    )
    assert c.coin_id == "ethereum"
    assert c.first_tracked_date is None  # Optional


def test_market_overview():
    """MarketOverview con datos mínimos."""
    m = MarketOverview(total_coins=8, total_fact_rows=5000)
    assert m.total_coins == 8


def test_fear_greed_response():
    """FearGreedResponse con todos los campos."""
    fg = FearGreedResponse(
        index_date=date(2025, 2, 1),
        fear_greed_value=25,
        classification="Extreme Fear",
    )
    assert fg.fear_greed_value == 25
    assert fg.classification == "Extreme Fear"


def test_health_response():
    """HealthResponse healthy."""
    h = HealthResponse(
        status="healthy",
        thrift_connected=True,
        tables_available=3,
    )
    assert h.status == "healthy"
