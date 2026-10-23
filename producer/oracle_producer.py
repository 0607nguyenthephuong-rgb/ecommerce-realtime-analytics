"""Publish committed Oracle outbox rows to Kafka with at-least-once delivery."""

import argparse
import os
import time

from confluent_kafka import Producer

from ingestion.oracle_payments import connect_app


def create_producer(bootstrap_servers):
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "ecommerce-oracle-outbox",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "snappy",
        "linger.ms": 20,
        "message.timeout.ms": 120000,
    })


SELECT_PENDING = """
SELECT outbox_id, topic, message_key, payload
  FROM payment_outbox
 WHERE published_at IS NULL
 ORDER BY created_at, outbox_id
 FOR UPDATE SKIP LOCKED
"""


def publish_one(connection, producer):
    with connection.cursor() as cursor:
        cursor.arraysize = 1
        cursor.execute(SELECT_PENDING)
        row = cursor.fetchone()
        if row is None:
            connection.rollback()
            return False
        outbox_id, topic, key, payload = row
        if hasattr(payload, "read"):
            payload = payload.read()
        delivery = []

        def callback(error, message):
            delivery.append(error)

        try:
            while True:
                try:
                    producer.produce(
                        topic=topic, key=str(key).encode("utf-8"),
                        value=payload.encode("utf-8"), on_delivery=callback,
                    )
                    break
                except BufferError:
                    producer.poll(0.1)
            pending = producer.flush(30)
            if pending or delivery != [None]:
                raise RuntimeError(
                    f"Kafka did not confirm outbox event {outbox_id}: {delivery}"
                )
            cursor.execute(
                "UPDATE payment_outbox SET published_at = SYSTIMESTAMP "
                "WHERE outbox_id = :outbox_id AND published_at IS NULL",
                outbox_id=outbox_id,
            )
            if cursor.rowcount != 1:
                raise RuntimeError(f"Outbox row changed during publish: {outbox_id}")
            connection.commit()
            print(f"Published {outbox_id} -> {topic}", flush=True)
            return True
        except Exception:
            connection.rollback()
            raise


def main():
    parser = argparse.ArgumentParser(description="Oracle outbox -> Kafka")
    parser.add_argument("--bootstrap-servers", default=os.environ.get(
        "ECOMMERCE_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--once", action="store_true",
                        help="publish pending rows and exit")
    args = parser.parse_args()
    if args.poll_seconds <= 0 or args.max_events < 0:
        parser.error("poll-seconds must be positive and max-events nonnegative")

    producer = create_producer(args.bootstrap_servers)
    published = 0
    with connect_app() as connection:
        print("Oracle outbox relay started", flush=True)
        while not args.max_events or published < args.max_events:
            if publish_one(connection, producer):
                published += 1
            elif args.once:
                break
            else:
                time.sleep(args.poll_seconds)
    print(f"Oracle outbox relay complete: published={published}", flush=True)


if __name__ == "__main__":
    main()
