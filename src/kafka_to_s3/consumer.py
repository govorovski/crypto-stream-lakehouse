import gzip
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO

import boto3
from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "trades_raw")
GROUP = os.getenv("KAFKA_GROUP_ID", "kafka-to-s3-dev")

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "landing/trades_raw/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

BATCH_SECONDS = int(os.getenv("BATCH_SECONDS", "10"))
MAX_RECORDS = int(os.getenv("MAX_RECORDS", "5000"))
STAT_EVERY = int(os.getenv("STAT_EVERY", "5"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

if not S3_BUCKET:
    raise SystemExit("S3_BUCKET env is required")

def setup_logger() -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
        force=True,
    )
    return logging.getLogger("kafka_to_s3")


logger = setup_logger()

def utc_now():
    return datetime.now(timezone.utc)

def make_s3_key(now: datetime) -> str:
    # s3://bucket/landing/trades_raw/date=YYYY-MM-DD/hour=HH/part-...jsonl.gz
    return (
        f"{S3_PREFIX}"
        f"date={now:%Y-%m-%d}/"
        f"hour={now:%H}/"
        f"part-{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex}.jsonl.gz"
    )

def get_lag(consumer: Consumer) -> int:
    lag_total = 0
    try:
        for tp in consumer.assignment():
            low, high = consumer.get_watermark_offsets(tp, cached=False)
            current_offset = max(tp.offset, 0)
            lag_total += max(high - current_offset, 0)
    except Exception:
        pass
    return lag_total

def flush_batch(s3_client, records: list[bytes]) -> tuple[str, int]:
    now = utc_now()
    key = make_s3_key(now)

    buf = BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for rec in records:
            gz.write(rec)
            gz.write(b"\n")

    payload = buf.getvalue()

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=payload,
        ContentType="application/json",
        ContentEncoding="gzip",
    )

    return key, len(payload)

def main():
    logger.info("consumer starting")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    s3 = boto3.client("s3", region_name=AWS_REGION)

    consumer.subscribe([TOPIC])

    buf: list[bytes] = []
    next_flush = time.time() + BATCH_SECONDS
    next_stat = time.time() + STAT_EVERY

    stat_msgs = 0
    stat_bytes = 0

    logger.info(
        "consuming topic=%s group=%s s3_bucket=%s s3_prefix=%s batch_seconds=%s max_records=%s",
        TOPIC,
        GROUP,
        S3_BUCKET,
        S3_PREFIX,
        BATCH_SECONDS,
        MAX_RECORDS,
    )

    try:
        while True:
            msg = consumer.poll(1.0)
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
                lag = get_lag(consumer)                
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
                key, payload_size = flush_batch(s3, buf)
                logger.info(
                    "flush wrote=%s payload_bytes=%s s3_path=s3://%s/%s",
                    len(buf),
                    payload_size,
                    S3_BUCKET,
                    key,
                )
                buf.clear()
                next_flush = now + BATCH_SECONDS

    except KeyboardInterrupt:
        logger.info("consumer interrupted")
    except Exception:
        logger.exception("consumer crashed")
        raise
    finally:
        logger.info("closing consumer")
        consumer.close()

if __name__ == "__main__":
    main()