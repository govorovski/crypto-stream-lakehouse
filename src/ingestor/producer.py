import json
import os
import time
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "trades_raw")
SYMBOLS = os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",")

def delivery_report(err, msg):
    if err is not None:
        print(f"[delivery] FAILED: {err}")
    else:
        print(f"[delivery] OK topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")

def main():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "ingestor-test",
        # It’s enough for testing.
        # all - Maximum reliability. The producer waits for acknowledgment from: the leader, all in-sync replicas (ISR). The producer will wait for acknowledgment of the write from ALL replicas of the partition.
        #0 - Does NOT wait for any acknowledgment.
        #1 - default - The producer waits for acknowledgment only from the partition leader.
        "acks": "all",
    })

    i = 0
    while True:
        symbol = SYMBOLS[i % len(SYMBOLS)].strip()
        payload = {
            "source": "test",
            "exchange": "local",
            "symbol": symbol,
            "trade_id": str(uuid.uuid4()),
            "event_ts": datetime.now(timezone.utc).isoformat(),
            "price": str(50000 + (i % 100)),
            "qty": "0.01",
            "is_buyer_maker": bool(i % 2),
        }
        value = json.dumps(payload).encode("utf-8")
        key = symbol.encode("utf-8")

        producer.produce(TOPIC, value=value, key=key, callback=delivery_report)
        # 0 sec - Check the event, don’t wait.
        producer.poll(0)   # delivery_report Callback handling
        i += 1
        time.sleep(1)

if __name__ == "__main__":
    main()