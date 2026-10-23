"""Oracle integration check for checkout amount, decline, and idempotency."""

import uuid

from ingestion.oracle_payments import connect_app, initialize_schema, process_event


def event(event_id, kind, product_id, price, outcome="succeeded", quantity=1):
    return {
        "event_id": event_id,
        "event_time": "2020-01-01T00:00:00+00:00",
        "event_type": kind,
        "product_id": product_id,
        "price": price,
        "user_id": 42,
        "quantity": quantity,
        "payment_outcome": outcome,
    }


def main():
    initialize_schema()
    token = uuid.uuid4().hex[:12]
    product_id = 900000000000 + int(token[:8], 16)
    event_ids = [f"payment-test-{token}-{n}" for n in range(3)]
    with connect_app() as connection:
        try:
            assert process_event(connection, event(
                event_ids[0], "view", product_id, "10.29"
            ), "analytics.test", "payments.test") == "activity"
            assert process_event(connection, event(
                event_ids[1], "purchase", product_id, "0.01", quantity=2
            ), "analytics.test", "payments.test") == "paid"
            assert process_event(connection, event(
                event_ids[1], "purchase", product_id, "0.01", quantity=2
            ), "analytics.test", "payments.test") == "duplicate"
            assert process_event(connection, event(
                event_ids[2], "purchase", product_id, "0.01", "declined"
            ), "analytics.test", "payments.test") == "declined"

            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT o.status,
                           TO_CHAR(o.total_amount, 'FM9999999999999999990D00',
                                   'NLS_NUMERIC_CHARACTERS = ''.,'''),
                           p.status,
                           TO_CHAR(p.amount, 'FM9999999999999999990D00',
                                   'NLS_NUMERIC_CHARACTERS = ''.,''')
                      FROM payment_orders o JOIN payment_transactions p
                        ON o.order_id = p.order_id
                     WHERE o.source_event_id IN (:one, :two)
                     ORDER BY o.source_event_id
                """, one=event_ids[1], two=event_ids[2])
                rows = cursor.fetchall()
                assert rows == [
                    ("PAID", "20.58", "SUCCEEDED", "20.58"),
                    ("PAYMENT_FAILED", "10.29", "DECLINED", "10.29"),
                ], rows
                cursor.execute("""
                    SELECT topic, COUNT(*) FROM payment_outbox
                     WHERE source_event_id IN (:one, :two, :three)
                     GROUP BY topic ORDER BY topic
                """, one=event_ids[0], two=event_ids[1], three=event_ids[2])
                assert cursor.fetchall() == [
                    ("analytics.test", 2), ("payments.test", 1)
                ]
            print("Oracle payment integration check passed", flush=True)
        finally:
            with connection.cursor() as cursor:
                binds = {"one": event_ids[0], "two": event_ids[1], "three": event_ids[2]}
                cursor.execute("""
                    DELETE FROM payment_outbox
                     WHERE source_event_id IN (:one, :two, :three)
                """, binds)
                cursor.execute("""
                    DELETE FROM payment_transactions WHERE order_id IN (
                        SELECT order_id FROM payment_orders
                         WHERE source_event_id IN (:one, :two, :three))
                """, binds)
                cursor.execute("""
                    DELETE FROM payment_orders
                     WHERE source_event_id IN (:one, :two, :three)
                """, binds)
                cursor.execute("""
                    DELETE FROM payment_ingested_events
                     WHERE event_id IN (:one, :two, :three)
                """, binds)
                cursor.execute(
                    "DELETE FROM payment_catalog WHERE product_id = :product_id",
                    product_id=product_id,
                )
            connection.commit()


if __name__ == "__main__":
    main()
