import gzip
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "trades_raw")
GROUP = os.getenv("KAFKA_GROUP_ID", "kafka-to-s3-dev")

LANDING_DIR = Path(os.getenv("LANDING_DIR", "/landing/trades_raw"))
BATCH_SECONDS = int(os.getenv("BATCH_SECONDS", "10"))
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "5000"))
STAT_EVERY = int(os.getenv("STAT_EVERY", "5"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

def setup_logger() -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
        force=True,
    )
    return logging.getLogger("kafka_to_landing")


logger = setup_logger()

def utc_now():
    return datetime.now(timezone.utc)

def rotate_path(now: datetime) -> Path:
    # /landing/trades_raw/date=YYYY-MM-DD/hour=HH/part-...jsonl.gz
    p = LANDING_DIR / f"date={now:%Y-%m-%d}" / f"hour={now:%H}"
    p.mkdir(parents=True, exist_ok=True)
    fname = f"part-{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex}.jsonl.gz"
    return p / fname

def get_lag(consumer):
    lag_total = 0
    try:
        for tp in consumer.assignment():
            low, high = consumer.get_watermark_offsets(tp)
            lag_total += max(high - tp.offset, 0)
    except Exception:
        pass
    return lag_total

def main():
    logger.info("consumer starting")
    LANDING_DIR.mkdir(parents=True, exist_ok=True)

    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC])

    buf: list[bytes] = []
    next_flush = time.time() + BATCH_SECONDS
    next_stat = time.time() + STAT_EVERY

    stat_msgs = 0
    stat_bytes = 0

    logger.info(
        "consuming topic=%s group=%s landing_dir=%s batch_seconds=%s max_records=%s",
        TOPIC,
        GROUP,
        LANDING_DIR,
        BATCH_SECONDS,
        MAX_RECORDS,
    )

    try:
        while True:
            msg = c.poll(1.0)
            now = time.time()

            if msg is not None:
                if msg.error():
                    logger.warning("kafka message error: %s", msg.error())
                else:
                    value = msg.value()
                    buf.append(value)
                    stat_msgs += 1
                    stat_bytes += len(value)

            if now >= next_stat:
                
                lag = get_lag(c)
                
                logger.info(
                    "stats in=%s msg/s=%.2f bytes=%s kib/s=%.2f buf=%s lag=%s",
                    stat_msgs,
                    stat_msgs / STAT_EVERY,
                    stat_bytes,
                    stat_bytes / STAT_EVERY / 1024,
                    len(buf),
                    lag,
                )
                stat_msgs = 0
                stat_bytes = 0
                next_stat = now + STAT_EVERY

            if buf and (now >= next_flush or len(buf) >= MAX_RECORDS):
                ts = utc_now()
                path = rotate_path(ts)
                with gzip.open(path, "wb") as f:
                    for rec in buf:
                        f.write(rec)
                        f.write(b"\n")

                logger.info("flush wrote=%s path=%s", len(buf), path)
                buf.clear()
                next_flush = now + BATCH_SECONDS

    except KeyboardInterrupt:
        logger.info("consumer interrupted")
    except Exception:
        logger.exception("consumer crashed")
        raise
    finally:
        logger.info("closing consumer")
        c.close()

if __name__ == "__main__":
    main()