"""Oracle transaction model for a simulated ecommerce checkout stream."""

import json
import os
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import oracledb

TABLES = (
    """CREATE TABLE payment_ingested_events (
        event_id VARCHAR2(64 CHAR) PRIMARY KEY,
        event_type VARCHAR2(32 CHAR) NOT NULL,
        received_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
    )""",
    """CREATE TABLE payment_catalog (
        product_id NUMBER(19, 0) PRIMARY KEY,
        unit_price NUMBER(18, 2) NOT NULL CHECK (unit_price >= 0),
        category_id VARCHAR2(32 CHAR),
        category_code VARCHAR2(255 CHAR),
        brand VARCHAR2(255 CHAR),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
    )""",
    """CREATE TABLE payment_orders (
        order_id VARCHAR2(36 CHAR) PRIMARY KEY,
        source_event_id VARCHAR2(64 CHAR) NOT NULL UNIQUE
            REFERENCES payment_ingested_events(event_id),
        user_id NUMBER(19, 0) NOT NULL,
        product_id NUMBER(19, 0) NOT NULL REFERENCES payment_catalog(product_id),
        quantity NUMBER(9, 0) NOT NULL CHECK (quantity > 0),
        unit_price NUMBER(18, 2) NOT NULL,
        total_amount NUMBER(20, 2) NOT NULL,
        status VARCHAR2(24 CHAR) NOT NULL
            CHECK (status IN ('PENDING', 'PAID', 'PAYMENT_FAILED')),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
    )""",
    """CREATE TABLE payment_transactions (
        payment_id VARCHAR2(36 CHAR) PRIMARY KEY,
        order_id VARCHAR2(36 CHAR) NOT NULL UNIQUE REFERENCES payment_orders(order_id),
        amount NUMBER(20, 2) NOT NULL,
        status VARCHAR2(24 CHAR) NOT NULL
            CHECK (status IN ('SUCCEEDED', 'DECLINED')),
        provider_reference VARCHAR2(80 CHAR),
        processed_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL
    )""",
    """CREATE TABLE payment_outbox (
        outbox_id VARCHAR2(64 CHAR) PRIMARY KEY,
        source_event_id VARCHAR2(64 CHAR) NOT NULL UNIQUE
            REFERENCES payment_ingested_events(event_id),
        topic VARCHAR2(255 CHAR) NOT NULL,
        message_key VARCHAR2(64 CHAR) NOT NULL,
        payload CLOB NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
        published_at TIMESTAMP WITH TIME ZONE
    )""",
)


def ensure_app_user(dsn, admin_password, app_password):
    if not app_password.isalnum() or len(app_password) < 8:
        raise ValueError("ORACLE_APP_PASSWORD must have at least 8 letters/digits")
    with oracledb.connect(
        user="system", password=admin_password, dsn=dsn
    ) as connection, connection.cursor() as cursor:
        try:
            cursor.execute(f'CREATE USER ecommerce IDENTIFIED BY "{app_password}"')
        except oracledb.DatabaseError as exc:
            if exc.args[0].code != 1920:  # user already exists
                raise
        cursor.execute("GRANT CREATE SESSION, CREATE TABLE TO ecommerce")
        cursor.execute("ALTER USER ecommerce QUOTA UNLIMITED ON USERS")


def connect_app():
    return oracledb.connect(
        user="ecommerce",
        password=os.environ.get("ORACLE_APP_PASSWORD", "EcommerceDev123"),
        dsn=os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1"),
    )


def initialize_schema():
    ensure_app_user(
        os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1"),
        os.environ.get("ORACLE_ADMIN_PASSWORD", "OracleDev123"),
        os.environ.get("ORACLE_APP_PASSWORD", "EcommerceDev123"),
    )
    with connect_app() as connection, connection.cursor() as cursor:
        for statement in TABLES:
            try:
                cursor.execute(statement)
                print(f"Created {statement.split()[2]}", flush=True)
            except oracledb.DatabaseError as exc:
                if exc.args[0].code != 955:  # table already exists
                    raise


def _positive_int(value, name):
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if result <= 0 or result >= 10**19 or str(result) != str(value):
        raise ValueError(f"{name} must be a positive integer")
    return result


def validate_event(event):
    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not 1 <= len(event_id) <= 64:
        raise ValueError("event_id must be a string of 1 to 64 characters")
    event_type = event.get("event_type")
    if not isinstance(event_type, str) or event_type not in {
        "view", "cart", "remove_from_cart", "purchase"
    }:
        raise ValueError("unsupported event_type")
    product_id = _positive_int(event.get("product_id"), "product_id")
    user_id = _positive_int(event.get("user_id"), "user_id")
    quantity = _positive_int(event.get("quantity", 1), "quantity")
    if quantity > 1_000_000:
        raise ValueError("quantity is too large")
    try:
        source_price = Decimal(str(event.get("price")))
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("price must be a decimal amount") from exc
    try:
        valid_amount = (
            source_price.is_finite() and source_price >= 0
            and source_price <= Decimal("9999999999999999.99")
            and source_price == source_price.quantize(Decimal("0.01"))
            and source_price * quantity <= Decimal("999999999999999999.99")
        )
    except InvalidOperation:
        valid_amount = False
    if not valid_amount:
        raise ValueError("price must be a nonnegative amount with two decimals")
    try:
        event_time = datetime.fromisoformat(
            str(event.get("event_time")).replace(" UTC", "+00:00").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError("invalid event_time") from exc
    if event_time.tzinfo is None:
        raise ValueError("event_time must have a timezone")
    outcome = event.get("payment_outcome", "succeeded")
    if not isinstance(outcome, str) or outcome not in {"succeeded", "declined"}:
        raise ValueError("payment_outcome must be succeeded or declined")
    for name, max_length in (("category_id", 32), ("category_code", 255),
                             ("brand", 255)):
        value = event.get(name)
        if value is not None and (not isinstance(value, str) or len(value) > max_length):
            raise ValueError(f"{name} must be text of at most {max_length} characters")
    return {
        "event_id": event_id, "event_type": event_type,
        "product_id": product_id, "user_id": user_id,
        "quantity": quantity, "source_price": source_price,
        "event_time": event_time.astimezone(timezone.utc),
        "outcome": outcome,
    }


def _stable_id(kind, event_id):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"ecommerce/{kind}/{event_id}"))


def process_event(connection, event, analytics_topic, payments_topic):
    """Atomically record input, calculate checkout, and enqueue a committed event.

    Replaying a CSV row with the same event_id has no further effect.
    """
    info = validate_event(event)
    event_id = info["event_id"]
    try:
        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    "INSERT INTO payment_ingested_events(event_id, event_type) "
                    "VALUES (:event_id, :event_type)",
                    event_id=event_id, event_type=info["event_type"],
                )
            except oracledb.DatabaseError as exc:
                if exc.args[0].code == 1:
                    connection.rollback()
                    return "duplicate"
                raise

            # The first incoming price seeds a demo catalogue. Later checkouts
            # use the price stored in Oracle, regardless of the incoming price.
            cursor.execute("""
                MERGE INTO payment_catalog target
                USING (SELECT :product_id AS product_id FROM dual) source
                ON (target.product_id = source.product_id)
                WHEN NOT MATCHED THEN INSERT (
                    product_id, unit_price, category_id, category_code, brand
                ) VALUES (
                    :product_id, :unit_price, :category_id, :category_code, :brand
                )
            """, product_id=info["product_id"],
                unit_price=info["source_price"],
                category_id=event.get("category_id"),
                category_code=event.get("category_code"),
                brand=event.get("brand"))

            payload = dict(event)
            payload["event_time"] = info["event_time"].isoformat()
            payload["product_id"] = info["product_id"]
            payload["user_id"] = info["user_id"]
            payload["ingestion_time"] = time.time()
            topic = analytics_topic

            if info["event_type"] == "purchase":
                order_id = _stable_id("order", event_id)
                payment_id = _stable_id("payment", event_id)
                cursor.execute("""
                    INSERT INTO payment_orders (
                        order_id, source_event_id, user_id, product_id,
                        quantity, unit_price, total_amount, status
                    ) SELECT :order_id, :event_id, :user_id, product_id,
                             :quantity, unit_price, unit_price * :quantity, 'PENDING'
                      FROM payment_catalog WHERE product_id = :product_id
                """, order_id=order_id, event_id=event_id,
                    user_id=info["user_id"], quantity=info["quantity"],
                    product_id=info["product_id"])
                cursor.execute(
                    "SELECT TO_CHAR(total_amount, "
                    "'FM9999999999999999990D00', "
                    "'NLS_NUMERIC_CHARACTERS = ''.,''') "
                    "FROM payment_orders WHERE order_id = :id",
                    id=order_id,
                )
                amount = Decimal(cursor.fetchone()[0])
                status = "SUCCEEDED" if info["outcome"] == "succeeded" else "DECLINED"
                cursor.execute("""
                    INSERT INTO payment_transactions (
                        payment_id, order_id, amount, status, provider_reference
                    ) VALUES (:payment_id, :order_id, :amount, :status, :provider_reference)
                """, payment_id=payment_id, order_id=order_id,
                    amount=amount, status=status,
                    provider_reference=f"SIM-{payment_id}" if status == "SUCCEEDED" else None)
                cursor.execute(
                    "UPDATE payment_orders SET status = :status WHERE order_id = :order_id",
                    status="PAID" if status == "SUCCEEDED" else "PAYMENT_FAILED",
                    order_id=order_id,
                )
                if status == "SUCCEEDED":
                    payload.update({
                        "price": str(amount), "quantity": info["quantity"],
                        "order_id": order_id, "payment_id": payment_id,
                        "payment_status": "SUCCEEDED",
                    })
                else:
                    topic = payments_topic
                    payload = {
                        "event_id": event_id, "event_time": payload["event_time"],
                        "event_type": "payment_failed", "user_id": info["user_id"],
                        "product_id": info["product_id"], "quantity": info["quantity"],
                        "amount": str(amount), "order_id": order_id,
                        "payment_id": payment_id, "payment_status": "DECLINED",
                    }
            else:
                payload["price"] = str(info["source_price"])

            cursor.execute("""
                INSERT INTO payment_outbox (
                    outbox_id, source_event_id, topic, message_key, payload
                ) VALUES (:outbox_id, :event_id, :topic, :message_key, :payload)
            """, outbox_id=event_id, event_id=event_id, topic=topic,
                message_key=str(info["user_id"]),
                payload=json.dumps(payload, ensure_ascii=False))
        connection.commit()
        return "paid" if info["event_type"] == "purchase" and info["outcome"] == "succeeded" else (
            "declined" if info["event_type"] == "purchase" else "activity"
        )
    except Exception:
        connection.rollback()
        raise
