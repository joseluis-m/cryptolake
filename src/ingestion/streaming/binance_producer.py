"""
Productor Kafka: Binance WebSocket → Kafka topic "prices.realtime"

¿Cómo funciona?
1. Se conecta al WebSocket público de Binance (gratis, sin API key)
2. Se suscribe al stream "aggTrade" (trades agregados) de cada par
3. Cada vez que alguien compra/vende BTC, ETH, etc., Binance nos envía el precio
4. Transformamos el mensaje al formato de CryptoLake
5. Lo publicamos en el topic de Kafka "prices.realtime"

Binance envía ~50-200 mensajes por SEGUNDO dependiendo de la actividad del mercado.

Para ejecutar:
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

# Configurar logger
logger = structlog.get_logger()

# ── Mapeo de símbolos ──────────────────────────────────────
# Binance usa "BTCUSDT", nosotros usamos "bitcoin" (formato CoinGecko).
# Este mapeo unifica los identificadores entre fuentes.
BINANCE_SYMBOLS = {
    "btcusdt": "bitcoin",
    "ethusdt": "ethereum",
    "solusdt": "solana",
    "adausdt": "cardano",
    "dotusdt": "polkadot",
    "linkusdt": "chainlink",
    "avaxusdt": "avalanche-2",
    "maticusdt": "matic-network",
}

# URL base del WebSocket de Binance
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"


def create_kafka_producer() -> Producer:
    """
    Crea y configura un productor de Kafka.

    Configuraciones importantes:
    - acks=all: El productor espera a que Kafka confirme que el mensaje
      fue escrito en TODAS las réplicas. Máxima durabilidad.
    - compression.type=snappy: Comprime los mensajes para reducir ancho
      de banda y espacio en disco. Snappy es rápido con buena compresión.
    - linger.ms=100: En vez de enviar cada mensaje individualmente,
      espera 100ms para agrupar varios mensajes en un solo envío (batch).
      Esto mejora el throughput a costa de 100ms de latencia extra.
    """
    config = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "binance-price-producer",
        "acks": "all",
        "compression.type": "snappy",
        "linger.ms": 100,
        "batch.size": 65536,  # 64KB de batch máximo
        "retries": 3,
        "retry.backoff.ms": 500,
    }

    logger.info(
        "kafka_producer_created",
        bootstrap_servers=settings.kafka_bootstrap_servers,
    )
    return Producer(config)


def delivery_callback(err, msg):
    """
    Callback que Kafka llama cuando un mensaje se entrega (o falla).

    ¿Por qué un callback? Porque la producción es asíncrona.
    Cuando llamas a producer.produce(), el mensaje se pone en un buffer
    interno. Kafka lo envía en background y llama a este callback
    cuando sabe si se entregó o no.
    """
    if err:
        logger.error(
            "kafka_delivery_failed",
            error=str(err),
            topic=msg.topic(),
        )


def transform_binance_trade(raw_data: dict) -> dict:
    """
    Transforma un mensaje raw de Binance a nuestro schema estándar.

    Binance aggTrade format (lo que recibimos):
    {
        "e": "aggTrade",     // tipo de evento
        "s": "BTCUSDT",      // símbolo del par
        "p": "67432.10",     // precio (STRING, no número)
        "q": "0.123",        // cantidad
        "T": 1708900000000,  // timestamp del trade (milisegundos)
        "E": 1708900000001,  // timestamp del evento
        "m": false           // ¿el comprador es el maker?
    }

    CryptoLake format (lo que producimos a Kafka):
    {
        "coin_id": "bitcoin",
        "symbol": "BTCUSDT",
        "price_usd": 67432.10,    // Convertido a float
        "quantity": 0.123,
        "trade_time_ms": 1708900000000,
        "ingested_at": "2025-01-15T10:30:00+00:00",
        "source": "binance_websocket"
    }
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
    """
    Loop principal: conecta a Binance WebSocket y produce a Kafka.

    Flujo:
    1. Construye la URL combinando todos los streams que queremos
    2. Se conecta al WebSocket
    3. Por cada mensaje recibido:
       a. Parsea el JSON
       b. Transforma al formato CryptoLake
       c. Produce a Kafka (con coin_id como key para particionado)
    4. Si se desconecta, espera 5 segundos y reconecta

    ¿Por qué coin_id como key de Kafka?
    Kafka garantiza que mensajes con la misma key van a la misma partición.
    Esto significa que todos los mensajes de "bitcoin" estarán en la misma
    partición, manteniendo el orden temporal de los trades de BTC.
    """
    # Importar websockets aquí para permitir importar el módulo sin tenerlo
    import websockets

    producer = create_kafka_producer()

    # Construir URL con todos los streams combinados
    # Formato: wss://stream.binance.com:9443/ws/btcusdt@aggTrade/ethusdt@aggTrade/...
    streams = "/".join(f"{symbol}@aggTrade" for symbol in BINANCE_SYMBOLS.keys())
    ws_url = f"{BINANCE_WS_URL}/{streams}"

    logger.info(
        "connecting_to_binance",
        symbols=list(BINANCE_SYMBOLS.keys()),
        num_pairs=len(BINANCE_SYMBOLS),
    )

    message_count = 0

    # Loop de reconexión: si se cae, reconecta automáticamente
    while True:
        try:
            async with websockets.connect(ws_url) as websocket:
                logger.info("websocket_connected", url=BINANCE_WS_URL)

                # Loop de lectura: procesa cada mensaje que llega
                async for raw_message in websocket:
                    try:
                        data = json.loads(raw_message)

                        # Binance combined streams envuelve en {"stream": ..., "data": {...}}
                        if "data" in data:
                            data = data["data"]

                        # Ignorar mensajes que no son trades
                        if data.get("e") != "aggTrade":
                            continue

                        # Transformar al formato CryptoLake
                        record = transform_binance_trade(data)

                        # Producir a Kafka
                        producer.produce(
                            topic=settings.kafka_topic_prices,
                            # Key: determina la partición. Mismo coin → misma partición
                            key=record["coin_id"].encode("utf-8"),
                            # Value: el mensaje completo en JSON
                            value=json.dumps(record).encode("utf-8"),
                            callback=delivery_callback,
                        )

                        message_count += 1

                        # Cada 500 mensajes: flush (forzar envío) y log de progreso
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
            # Flush mensajes pendientes antes de reconectar
            producer.flush()
            await asyncio.sleep(5)


def main():
    """Punto de entrada principal."""
    logger.info("binance_producer_starting")

    # Manejar Ctrl+C limpiamente
    def signal_handler(sig, frame):
        logger.info("shutting_down", total_messages=0)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    # Ejecutar el loop de streaming
    asyncio.run(stream_prices())


if __name__ == "__main__":
    main()
