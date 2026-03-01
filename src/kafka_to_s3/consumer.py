import os
from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "trades_raw")
GROUP = os.getenv("KAFKA_GROUP_ID", "kafka-to-s3-dev")

def main():
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    c.subscribe([TOPIC])
    print(f"Consuming from {TOPIC} (bootstrap={BOOTSTRAP}, group={GROUP}) ...")

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[error] {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            val = msg.value().decode("utf-8") if msg.value() else None
            print(f"[msg] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} key={key} val={val}")
    except KeyboardInterrupt:
        pass
    finally:
        c.close()

if __name__ == "__main__":
    main()