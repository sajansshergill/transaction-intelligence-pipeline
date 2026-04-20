"""
producer.py — lightweight transaction event generator.

This module is intentionally runnable in environments without Kafka/Java.

Default behavior (no Kafka args): emit JSON lines to stdout ("dry run") so
developers can validate downstream parsing without infrastructure.

If you provide --kafka-bootstrap and --topic, it will try to publish messages
using kafka-python (optional dependency).
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class TransactionEvent:
    transaction_id: str
    customer_id: str
    merchant_id: str
    merchant_name: str
    merchant_category: str
    city: str
    state: str
    amount_usd: float
    currency: str
    account_type: str
    transaction_type: str
    channel: str
    event_time: str
    kafka_ingest_time: str
    is_international: bool
    zip_code: str


MERCHANT_CATEGORIES = [
    "grocery",
    "restaurant",
    "gas_station",
    "pharmacy",
    "retail_clothing",
    "electronics",
    "travel_hotel",
    "travel_airline",
    "entertainment",
    "healthcare",
    "subscription_services",
    "atm",
]

ACCOUNT_TYPES = ["checking", "credit", "debit"]
TXN_TYPES = ["purchase", "refund", "chargeback", "atm_withdrawal"]
CHANNELS = ["in_store", "online", "mobile", "contactless"]


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def generate_event(seed: int | None = None) -> TransactionEvent:
    rng = random.Random(seed)
    category = rng.choice(MERCHANT_CATEGORIES)
    return TransactionEvent(
        transaction_id=str(uuid.uuid4()),
        customer_id=f"CUST_{rng.randint(100000, 999999)}",
        merchant_id=f"MER_{rng.randint(1000, 9999)}",
        merchant_name=rng.choice(["Corner Mart", "Metro Fuel", "Skyline Eats", "PharmaPlus", "TravelHub"]),
        merchant_category=category,
        city=rng.choice(["New York", "San Francisco", "Austin", "Seattle", "Chicago"]),
        state=rng.choice(["NY", "CA", "TX", "WA", "IL"]),
        amount_usd=round(rng.uniform(1.00, 500.00), 2),
        currency="USD",
        account_type=rng.choice(ACCOUNT_TYPES),
        transaction_type=rng.choice(TXN_TYPES),
        channel=rng.choice(CHANNELS),
        event_time=_iso_now(),
        kafka_ingest_time=_iso_now(),
        is_international=bool(rng.randint(0, 1)),
        zip_code=str(rng.randint(10000, 99999)),
    )


def _emit_stdout(event: TransactionEvent) -> None:
    sys.stdout.write(json.dumps(asdict(event)) + "\n")
    sys.stdout.flush()


def _emit_kafka(bootstrap: str, topic: str, event: TransactionEvent) -> None:
    try:
        from kafka import KafkaProducer  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Kafka publishing requested but kafka-python is not installed. "
            "Install it (pip install kafka-python) or run without --kafka-bootstrap."
        ) from e

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=50,
    )
    try:
        producer.send(topic, key=event.customer_id, value=asdict(event)).get(timeout=10)
        producer.flush(timeout=10)
    finally:
        producer.close(timeout=10)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate and optionally publish transaction events.")
    parser.add_argument("--count", type=int, default=10, help="Number of events to generate")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Sleep between events (ms)")
    parser.add_argument("--seed", type=int, default=None, help="Deterministic RNG seed (optional)")
    parser.add_argument("--kafka-bootstrap", default=None, help="Kafka bootstrap host:port (optional)")
    parser.add_argument("--topic", default=None, help="Kafka topic (required if --kafka-bootstrap is set)")
    args = parser.parse_args(argv)

    if args.kafka_bootstrap and not args.topic:
        parser.error("--topic is required when --kafka-bootstrap is provided")

    for i in range(args.count):
        event = generate_event(None if args.seed is None else (args.seed + i))
        if args.kafka_bootstrap:
            _emit_kafka(args.kafka_bootstrap, args.topic, event)
        else:
            _emit_stdout(event)

        if args.sleep_ms > 0 and i < args.count - 1:
            time.sleep(args.sleep_ms / 1000.0)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
