"""
Kafka Producer: Binance WebSocket -> Kafka topic "prices.realtime"

Connects to Binance's public WebSocket (no API key needed), subscribes to
aggTrade streams for tracked pairs, transforms messages to CryptoLake format,
and publishes them to Kafka.

Binance sends ~50-200 messages per SECOND depending on market activity.

Usage:
    python -m src.ingestion.streaming.binance_producer
"""

import asyncio
import json
import signal
import sys
from datetime import datetime, timezone

import structlog
from confluent_kafka import Producer

from src.config.settings import settings

logger = structlog.get_logger()

# Binance symbol -> CoinGecko ID mapping.
# Must match TRACKED_COINS in .env.example and settings.py.
BINANCE_SYMBOLS = {
    "btcusdt": "bitcoin",
    "ethusdt": "ethereum",
    "solusdt": "solana",
    "hypeusdt": "hyperliquid",
    "linkusdt": "chainlink",
    "uniusdt": "uniswap",
    "aaveusdt": "aave",
    "taousdt": "bittensor",
    "ondousdt": "ondo-finance",
}

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"


def create_kafka_producer() -> Producer:
    """Create and configure a Kafka producer."""
    config = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "binance-price-producer",
        "acks": "all",
        "compression.type": "snappy",
        "linger.ms": 100,
        "batch.size": 65536,
        "retries": 3,
        "retry.backoff.ms": 500,
    }

    logger.info(
        "kafka_producer_created",
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )
    return Producer(config)


def delivery_callback(err, msg):
    """Callback invoked on message delivery (or failure)."""
    if err:
        logger.error(
            "kafka_delivery_failed",
            error=str(err),
            topic=msg.topic(),
        )


def transform_binance_trade(raw_data: dict) -> dict:
    """Transform a raw Binance aggTrade message to CryptoLake format.

    Binance aggTrade input:
        {"e": "aggTrade", "s": "BTCUSDT", "p": "67432.10", "q": "0.123",
         "T": 1708900000000, "E": 1708900000001, "m": false}

    CryptoLake output:
        {"coin_id": "bitcoin", "symbol": "BTCUSDT", "price_usd": 67432.10,
         "quantity": 0.123, "trade_time_ms": 1708900000000, ...}
    """
    symbol_lower = raw_data.get("s", "").lower()
    coin_id = BINANCE_SYMBOLS.get(symbol_lower, symbol_lower)

    return {
        "coin_id": coin_id,
        "symbol": raw_data.get("s", ""),
        "price_usd": float(raw_data.get("p", 0)),
        "quantity": float(raw_data.get("q", 0)),
        "trade_time_ms": raw_data.get("T", 0),
        "event_time_ms": raw_data.get("E", 0),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "binance_websocket",
        "is_buyer_maker": raw_data.get("m", False),
    }


async def stream_prices():
    """Main loop: connect to Binance WebSocket and produce to Kafka.

    Uses coin_id as Kafka key to ensure all trades for the same coin
    land in the same partition, preserving temporal ordering.
    Reconnects automatically on disconnection with 5s backoff.
    """
    import websockets

    producer = create_kafka_producer()

    # Combined stream URL: btcusdt@aggTrade/ethusdt@aggTrade/...
    streams = "/".join(f"{symbol}@aggTrade" for symbol in BINANCE_SYMBOLS.keys())
    ws_url = f"{BINANCE_WS_URL}/{streams}"

    logger.info(
        "connecting_to_binance",
        symbols=list(BINANCE_SYMBOLS.keys()),
        num_pairs=len(BINANCE_SYMBOLS),
    )

    message_count = 0

    while True:
        try:
            async with websockets.connect(ws_url) as websocket:
                logger.info("websocket_connected", url=BINANCE_WS_URL)

                async for raw_message in websocket:
                    try:
                        data = json.loads(raw_message)

                        # Combined streams wrap data in {"stream": ..., "data": {...}}
                        if "data" in data:
                            data = data["data"]

                        if data.get("e") != "aggTrade":
                            continue

                        record = transform_binance_trade(data)

                        producer.produce(
                            topic=settings.kafka_topic_prices,
                            key=record["coin_id"].encode("utf-8"),
                            value=json.dumps(record).encode("utf-8"),
                            callback=delivery_callback,
                        )

                        message_count += 1

                        if message_count % 500 == 0:
                            producer.flush()
                            logger.info(
                                "streaming_progress",
                                total_messages=message_count,
                                last_coin=record["coin_id"],
                                last_price=record["price_usd"],
                            )

                    except json.JSONDecodeError as e:
                        logger.warning("json_parse_error", error=str(e))
                    except (KeyError, ValueError, TypeError) as e:
                        logger.warning("transform_error", error=str(e))

        except Exception as e:
            logger.warning(
                "websocket_disconnected",
                error=str(e),
                reconnecting_in="5s",
                total_messages_so_far=message_count,
            )
            producer.flush()
            await asyncio.sleep(5)


def main():
    """Entry point."""
    logger.info("binance_producer_starting")

    def signal_handler(sig, frame):
        logger.info("shutting_down", total_messages=0)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    asyncio.run(stream_prices())


if __name__ == "__main__":
    main()
