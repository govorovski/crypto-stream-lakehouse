import asyncio
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import websockets
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "trades_raw")

# comma-separated like: BTCUSDT,ETHUSDT
SYMBOLS = [s.strip().lower() for s in os.getenv("SYMBOLS", "btcusdt,ethusdt").split(",") if s.strip()]
SOURCE = os.getenv("SOURCE", "binance")
EXCHANGE = os.getenv("EXCHANGE", "binance_spot")
STAT_EVERY = int(os.getenv("STAT_EVERY", "5"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Binance combined streams
WS_URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(f"{s}@trade" for s in SYMBOLS)

def setup_logger() -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
        force=True,
    )
    return logging.getLogger("ingestor")

logger = setup_logger()

# Simple process-local metrics for the delivery callback.
DELIVERY_OK = 0
DELIVERY_FAIL = 0

def delivery_report(err, msg) -> None:
    global DELIVERY_OK, DELIVERY_FAIL

    if err is not None:
        DELIVERY_FAIL += 1
        logger.warning(
            "delivery failed topic=%s partition=%s error=%s",
            getattr(msg, "topic", lambda: "?")(),
            getattr(msg, "partition", lambda: "?")(),
            err,
        )
    else:
        DELIVERY_OK += 1


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat() #YYYY-MM-DDTHH:MM:SS.mmmmmm+HH:MM

def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "ingestor-binance",
        # It’s enough for testing.
        # all - Maximum reliability. The producer waits for acknowledgment from: the leader, all in-sync replicas (ISR). The producer will wait for acknowledgment of the write from ALL replicas of the partition.
        #0 - Does NOT wait for any acknowledgment.
        #1 - default - The producer waits for acknowledgment only from the partition leader.
        "acks": "all",
        # So it doesn’t crash because of brief disconnects.
        "retries": 10,
        "retry.backoff.ms": 500,
        "message.timeout.ms": 30000,
    })

async def run() -> None:
    global DELIVERY_OK, DELIVERY_FAIL

    producer = make_producer()

    stat_msgs = 0
    stat_bytes = 0
    started_at = time.time()
    next_stat = time.time() + STAT_EVERY

    logger.info(
        "producer starting bootstrap=%s topic=%s symbols=%s source=%s exchange=%s stat_every=%s",
        BOOTSTRAP,
        TOPIC,
        ",".join(SYMBOLS),
        SOURCE,
        EXCHANGE,
        STAT_EVERY,
    )

    while True:
        try:
            logger.info("ws connecting url=%s", WS_URL)

            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                logger.info("ws connected")

                async for raw in ws:
                    ingest_ts = utc_now_iso()
                    data = json.loads(raw)

                    # combined stream envelope: {"stream":"btcusdt@trade","data":{...}}
                    trade = data.get("data", data)
                    symbol = (trade.get("s") or "").upper()

                    # Binance fields:
                    # t tradeId, T tradeTime(ms), p price, q qty, m isBuyerMaker
                    event_ts = (
                        datetime.fromtimestamp(trade["T"] / 1000, tz=timezone.utc).isoformat()
                        if "T" in trade
                        else ingest_ts
                    )
                    trade_id = str(trade.get("t") or uuid.uuid4())

                    out = {
                        "source": SOURCE,
                        "exchange": EXCHANGE,
                        "symbol": symbol,
                        "trade_id": trade_id,
                        "event_ts": event_ts,
                        "ingest_ts": ingest_ts,
                        "price": str(trade.get("p")),
                        "qty": str(trade.get("q")),
                        "is_buyer_maker": bool(trade.get("m")),
                        "raw": trade,   # raw payload (dict)
                    }

                    key = symbol.encode("utf-8") if symbol else None
                    value = json.dumps(out, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

                    producer.produce(TOPIC, key=key, value=value, callback=delivery_report)
                    #A delivery report confirmation means:
                    # The message has been successfully written to Kafka (to the broker).
                    producer.poll(0)

                    stat_msgs += 1
                    stat_bytes += len(value)

                    now = time.time()
                    if now >= next_stat:
                        elapsed = max(now - started_at, 1e-6)
                        interval = STAT_EVERY

                        logger.info(
                            (
                                "stats produced=%s msg/s=%.2f bytes=%s kib/s=%.2f "
                                "acks=%s ack_rate=%.2f fails=%s queue=%s uptime_s=%.1f"
                            ),
                            stat_msgs,
                            stat_msgs / interval,
                            stat_bytes,
                            stat_bytes / interval / 1024,
                            DELIVERY_OK,
                            DELIVERY_OK / elapsed,
                            DELIVERY_FAIL,
                            len(producer),
                            elapsed,
                        )

                        stat_msgs = 0
                        stat_bytes = 0
                        next_stat = now + STAT_EVERY

        except Exception:
            logger.exception("ws loop failed; reconnecting in 5s")

            try:
                remaining = len(producer)
                logger.info("flushing producer before reconnect queued_messages=%s", remaining)
                # Wait up to 5 seconds for the producer to send everything accumulated in the internal buffer.
                producer.flush(5)
            except Exception:
                logger.exception("producer flush before reconnect failed")

            await asyncio.sleep(5)

def main():
    if not SYMBOLS:
        raise SystemExit("SYMBOLS env is empty")
    asyncio.run(run())

if __name__ == "__main__":
    main()