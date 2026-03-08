import os
import uuid
import random
import time
import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

BOOTSTRAP_SERVERS   = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
TOPIC_RAW           = os.environ["KAFKA_TOPIC_RAW"]
TOPIC_DLQ           = os.environ["KAFKA_TOPIC_DLQ"]
TPS                 = int(os.environ.get("PRODUCER_TPS", 100))

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":%(message)s}'
)
log = logging.getLogger(__name__)

MERCHANTS = [
    {"id": "M001", "name": "amazon",        "country": "US", "city": "Seattle"},
    {"id": "M002", "name": "jumia",          "country": "NG", "city": "Lagos"},
    {"id": "M003", "name": "uber",           "country": "US", "city": "NYC"},
    {"id": "M004", "name": "netflix",        "country": "US", "city": "LA"},
    {"id": "M005", "name": "shell",          "country": "GB", "city": "London"},
    {"id": "M006", "name": "binance",        "country": "MT", "city": "Valletta"},
    {"id": "M007", "name": "localbitcoins",  "country": "FI", "city": "Helsinki"},
    {"id": "M008", "name": "walmart",        "country": "US", "city": "Bentonville"},
    {"id": "M009", "name": "mcdonalds",      "country": "US", "city": "Chicago"},
    {"id": "M010", "name": "western_union",  "country": "US", "city": "Denver"},
]

DEVICES  = ["mobile", "web", "atm", "pos"]
USERS    = [f"U{i:04d}" for i in range(1, 201)]


def make_transaction() -> dict:
    merchant = random.choice(MERCHANTS)
    return {
        "transaction_id":   str(uuid.uuid4()),
        "user_id":          random.choice(USERS),
        "amount":           round(random.uniform(1.0, 5000.0), 2),
        "currency":         random.choice(["USD", "NGN", "GBP", "EUR"]),
        "merchant_id":      merchant["id"],
        "merchant_name":    merchant["name"],
        "country":          merchant["country"],
        "city":             merchant["city"],
        "device_id":        f"D{random.randint(100, 999)}",
        "device_type":      random.choice(DEVICES),
        "event_time":       int(datetime.now(timezone.utc).timestamp() * 1000),
        "is_international": random.random() < 0.15,
        "metadata":         {},
    }


def on_delivery(err, msg):
    if err:
        log.error(f'{{"event":"delivery_failed","error":"{err}"}}')


def build_serializer(sr_client: SchemaRegistryClient) -> AvroSerializer:
    schema_str = open("schemas/transaction.avsc").read()
    return AvroSerializer(sr_client, schema_str)


def run(producer: Producer, serializer: AvroSerializer, interval: float):
    sent = 0
    log.info(f'{{"event":"producer_start","tps":{TPS},"topic":"{TOPIC_RAW}"}}')

    while True:
        tx = make_transaction()
        try:
            producer.produce(
                topic=TOPIC_RAW,
                key=tx["user_id"],
                value=serializer(tx, SerializationContext(TOPIC_RAW, MessageField.VALUE)),
                on_delivery=on_delivery,
            )
            sent += 1
            if sent % 1000 == 0:
                producer.flush()
                log.info(f'{{"event":"heartbeat","sent":{sent}}}')
        except Exception as e:
            log.error(f'{{"event":"serialization_error","error":"{e}","tx_id":"{tx["transaction_id"]}"}}')
            producer.produce(
                topic=TOPIC_DLQ,
                key=tx["user_id"],
                value=json.dumps(tx).encode(),
            )
        time.sleep(interval)


def main():
    sr_client  = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    serializer = build_serializer(sr_client)
    producer   = Producer({
        "bootstrap.servers":  BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "acks":               "all",
        "linger.ms":          10,
        "batch.size":         65536,
    })
    run(producer, serializer, interval=1.0 / TPS)


if __name__ == "__main__":
    main()