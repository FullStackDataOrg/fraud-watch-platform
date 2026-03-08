# Fraud Detection Analytics Platform
**Real-Time Analytics Engineer Portfolio Project**

---

## Project Goal
Build a production-grade real-time fraud detection platform across 5 phases.
Each phase is independently deployable and builds on the previous.

**Stack:** Kafka · Spark Structured Streaming · Delta Lake · MinIO · dbt · Airflow · MLflow · Redis · FastAPI · Grafana · Superset · Docker Compose

---

## Coding Standards (Apply Everywhere)

```
- Functions: single responsibility, < 30 lines
- No inline comments explaining what code does — code should be self-documenting
- Comments only for WHY (non-obvious decisions)
- Config via environment variables, never hardcoded
- All secrets in .env, never in code
- Type hints on all Python functions
- Error handling: explicit, never silent except with logged reason
- Logs: structured JSON (timestamp, level, service, message, context)
```

---

## Repository Structure

```
fraud-platform/
├── CLAUDE.md
├── .env.example
├── docker-compose.yml              # Grows each phase
├── Makefile                        # make up / make down / make test / make logs
│
├── phase1-ingestion/
│   ├── producer/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── producer.py
│   │   └── schemas/
│   │       └── transaction.avsc
│   └── tests/
│       └── test_producer.py
│
├── phase2-streaming/
│   ├── spark_jobs/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── bronze_writer.py        # Kafka → Delta Bronze
│   │   └── quality_checks.py
│   └── tests/
│       └── test_bronze_writer.py
│
├── phase3-transforms/
│   ├── dbt_project/
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml
│   │   ├── models/
│   │   │   ├── staging/            # Bronze sources
│   │   │   ├── intermediate/       # Silver: cleaned + enriched
│   │   │   └── marts/              # Gold: analytics-ready
│   │   ├── tests/
│   │   ├── seeds/
│   │   └── macros/
│   └── airflow/
│       ├── Dockerfile
│       └── dags/
│           └── fraud_pipeline.py
│
├── phase4-ml/
│   ├── training/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── train.py
│   │   ├── features.py
│   │   └── evaluate.py
│   ├── serving/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── main.py                 # FastAPI app
│   │   ├── predictor.py
│   │   └── feature_client.py      # Redis feature lookups
│   └── tests/
│       ├── test_features.py
│       └── test_serving.py
│
├── phase5-analytics/
│   ├── grafana/
│   │   ├── provisioning/
│   │   │   ├── dashboards/
│   │   │   └── datasources/
│   │   └── dashboards/
│   │       ├── pipeline_health.json
│   │       └── ml_monitoring.json
│   └── superset/
│       └── superset_config.py
│
└── infra/
    ├── minio/
    │   └── init_buckets.sh
    └── trino/
        └── catalog/
            └── delta.properties
```

---

## Docker Compose — Service Addition Per Phase

Each phase appends services to `docker-compose.yml`. Never remove services from previous phases.

### Phase 1 Services
| Service | Image | Port | Purpose |
|---|---|---|---|
| kraft | kraft:latest | 2181 | Kafka coordination |
| kafka | confluentinc/cp-kafka:7.7.0 | 9092 | Event broker |
| schema-registry | confluentinc/cp-schema-registry:7.7.0 | 8081 | Avro schema management |
| kafka-ui | provectuslabs/kafka-ui:latest | 8085 | Topic monitoring |
| transaction-producer | ./phase1-ingestion/producer | — | Simulates transactions |

### Phase 2 Services (add to Phase 1)
| Service | Image | Port | Purpose |
|---|---|---|---|
| minio | minio/minio:latest | 9000/9001 | S3-compatible Delta Lake storage |
| spark-master | bitnami/spark:3.5 | 7077/8090 | Spark master |
| spark-worker | bitnami/spark:3.5 | — | Spark worker |
| spark-streaming | ./phase2-streaming | — | Bronze writer job |

### Phase 3 Services (add to Phase 2)
| Service | Image | Port | Purpose |
|---|---|---|---|
| postgres | postgres:15 | 5432 | Airflow metadata + dbt profiles |
| airflow | apache/airflow:2.8.0 | 8082 | Pipeline orchestration |
| trino | trinodb/trino:435 | 8083 | SQL over Delta Lake |
| dbt-runner | ./phase3-transforms/dbt_project | — | dbt model runs |

### Phase 4 Services (add to Phase 3)
| Service | Image | Port | Purpose |
|---|---|---|---|
| mlflow | ./phase4-ml/mlflow | 5000 | Experiment tracking + registry |
| redis | redis:7-alpine | 6379 | Real-time feature store |
| ml-training | ./phase4-ml/training | — | Model training (Airflow-triggered) |
| fraud-api | ./phase4-ml/serving | 8000 | Prediction endpoint |

### Phase 5 Services (add to Phase 4)
| Service | Image | Port | Purpose |
|---|---|---|---|
| prometheus | prom/prometheus:latest | 9090 | Metrics collection |
| grafana | grafana/grafana:latest | 3000 | Operational dashboards |
| superset | apache/superset:latest | 8088 | Business BI layer |

---

## Environment Variables (.env.example)

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
SCHEMA_REGISTRY_URL=http://schema-registry:8081
KAFKA_TOPIC_RAW=transactions.raw
KAFKA_TOPIC_ENRICHED=transactions.enriched
KAFKA_TOPIC_DLQ=transactions.dlq
PRODUCER_TPS=500

# MinIO / Storage
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
MINIO_ENDPOINT=http://minio:9000
DELTA_LAKE_BUCKET=fraud-platform
BRONZE_PATH=s3a://fraud-platform/bronze
SILVER_PATH=s3a://fraud-platform/silver
GOLD_PATH=s3a://fraud-platform/gold

# Spark
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_CHECKPOINT_PATH=s3a://fraud-platform/checkpoints

# Postgres (Airflow + dbt)
POSTGRES_USER=platform
POSTGRES_PASSWORD=platform123
POSTGRES_DB=fraud_platform
POSTGRES_HOST=postgres

# Airflow
AIRFLOW_FERNET_KEY=your-fernet-key-here
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin123

# MLflow
MLFLOW_TRACKING_URI=http://mlflow:5000
MLFLOW_EXPERIMENT_NAME=fraud_detection
MODEL_NAME=fraud_classifier
MIN_AUC_FOR_PROMOTION=0.90

# Redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_FEATURE_TTL=86400

# Fraud API
FRAUD_API_PORT=8000
MODEL_STAGE=Production
AB_CHALLENGER_PCT=0.10
```

---

## Makefile

```makefile
.PHONY: up down logs test build ps clean

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f $(service)

build:
	docker compose build

ps:
	docker compose ps

test:
	cd phase1-ingestion && pytest tests/ -v
	cd phase2-streaming && pytest tests/ -v
	cd phase3-transforms/dbt_project && dbt test
	cd phase4-ml && pytest tests/ -v

clean:
	docker compose down -v --remove-orphans

phase1-up:
	docker compose up -d zookeeper kafka schema-registry kafka-ui transaction-producer

phase2-up:
	docker compose up -d minio spark-master spark-worker spark-streaming

phase3-up:
	docker compose up -d postgres airflow trino dbt-runner

phase4-up:
	docker compose up -d mlflow redis ml-training fraud-api

phase5-up:
	docker compose up -d prometheus grafana superset
```

---

## Networks

```yaml
# In docker-compose.yml
networks:
  ingestion-net:      # Kafka, producer, schema-registry
  processing-net:     # Spark, MinIO
  serving-net:        # API, Redis, MLflow, dashboards
```

Services span networks only as needed (e.g., Spark bridges ingestion-net and processing-net).

---

---

# PHASE 1 — Data Ingestion Infrastructure

**Duration:** 2 weeks | **Goal:** Kafka stack running, transactions flowing, schema enforced

## What You're Building
```
[Python Producer] → [Kafka] ← [Schema Registry]
                          ↓
                    [Kafka UI] (monitoring)
                          ↓
                    [DLQ Topic] (bad messages)
```

## Key Concepts This Phase
- **Data Contracts:** Avro schema registered before any messages are sent
- **Schema Evolution:** Backward compatibility — consumers can read old + new messages
- **Dead Letter Queue:** Bad messages never block the pipeline; they're routed and inspectable
- **Partitioning:** 10 partitions = 10 parallel consumers maximum

## File Targets

### `phase1-ingestion/producer/schemas/transaction.avsc`
```json
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.fraudplatform",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "user_id",        "type": "string"},
    {"name": "amount",         "type": "double"},
    {"name": "currency",       "type": "string"},
    {"name": "merchant_id",    "type": "string"},
    {"name": "merchant_name",  "type": "string"},
    {"name": "country",        "type": "string"},
    {"name": "city",           "type": "string"},
    {"name": "device_id",      "type": "string"},
    {"name": "device_type",    "type": {"type": "enum", "name": "DeviceType", "symbols": ["mobile", "web", "atm", "pos"]}},
    {"name": "event_time",     "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "is_international","type": "boolean"},
    {"name": "metadata",       "type": {"type": "map", "values": "string"}, "default": {}}
  ]
}
```

### `phase1-ingestion/producer/producer.py`
```python
"""
Transaction producer: generates realistic payment events → Kafka
Config via env vars. Runs until stopped.
"""
import os, uuid, random, time, json, logging
from datetime import datetime, timezone
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# --- Config ---
BOOTSTRAP_SERVERS  = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
TOPIC_RAW          = os.environ["KAFKA_TOPIC_RAW"]
TOPIC_DLQ          = os.environ["KAFKA_TOPIC_DLQ"]
TPS                = int(os.environ.get("PRODUCER_TPS", 100))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

MERCHANTS = [
    {"id": "M001", "name": "Amazon", "country": "US"},
    {"id": "M002", "name": "Jumia",  "country": "NG"},
    {"id": "M003", "name": "Uber",   "country": "US"},
    {"id": "M004", "name": "Netflix","country": "US"},
    {"id": "M005", "name": "Shell",  "country": "GB"},
]

def make_transaction() -> dict:
    merchant = random.choice(MERCHANTS)
    return {
        "transaction_id":  str(uuid.uuid4()),
        "user_id":         f"U{random.randint(1000, 9999)}",
        "amount":          round(random.uniform(1.0, 5000.0), 2),
        "currency":        random.choice(["USD", "NGN", "GBP", "EUR"]),
        "merchant_id":     merchant["id"],
        "merchant_name":   merchant["name"],
        "country":         merchant["country"],
        "city":            random.choice(["Lagos", "London", "NYC", "Berlin"]),
        "device_id":       f"D{random.randint(100, 999)}",
        "device_type":     random.choice(["mobile", "web", "atm", "pos"]),
        "event_time":      int(datetime.now(timezone.utc).timestamp() * 1000),
        "is_international": random.random() < 0.15,
        "metadata":        {},
    }

def delivery_report(err, msg):
    if err:
        log.error({"event": "delivery_failed", "error": str(err)})

def main():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    schema_str = open("schemas/transaction.avsc").read()
    serializer = AvroSerializer(sr_client, schema_str)

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    interval = 1.0 / TPS
    sent = 0

    log.info({"event": "producer_start", "tps": TPS, "topic": TOPIC_RAW})

    while True:
        tx = make_transaction()
        try:
            producer.produce(
                topic=TOPIC_RAW,
                key=tx["user_id"],
                value=serializer(tx, SerializationContext(TOPIC_RAW, MessageField.VALUE)),
                on_delivery=delivery_report,
            )
            sent += 1
            if sent % 1000 == 0:
                log.info({"event": "heartbeat", "sent": sent})
                producer.flush()
        except Exception as e:
            log.error({"event": "serialization_error", "error": str(e), "tx_id": tx["transaction_id"]})
            producer.produce(TOPIC_DLQ, key=tx["user_id"], value=json.dumps(tx).encode())
        time.sleep(interval)

if __name__ == "__main__":
    main()
```

### `phase1-ingestion/producer/Dockerfile`
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "producer.py"]
```

### `phase1-ingestion/producer/requirements.txt`
```
confluent-kafka[avro]==2.3.0
requests==2.31.0
```

## Phase 1 — docker-compose.yml (starter)
```yaml
version: "3.9"

networks:
  ingestion-net:
  processing-net:
  serving-net:

services:

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    networks: [ingestion-net]
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 10s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    networks: [ingestion-net]
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:29092"]
      interval: 15s
      retries: 10

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    depends_on: [kafka]
    ports: ["8081:8081"]
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
    networks: [ingestion-net]

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on: [kafka, schema-registry]
    ports: ["8080:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: fraud-platform
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081
    networks: [ingestion-net]

  transaction-producer:
    build: ./phase1-ingestion/producer
    depends_on: [kafka, schema-registry]
    env_file: .env
    restart: unless-stopped
    networks: [ingestion-net]
```

## Phase 1 Validation Checklist
```
□ kafka-ui at localhost:8080 shows topic: transactions.raw
□ Messages arriving with Avro schema (not raw JSON)
□ Producer logs show heartbeat every 1000 messages
□ Schema registered at: http://localhost:8081/subjects
□ Sending a bad message routes to transactions.dlq
□ make test passes for phase1
```

## What You'll Know After Phase 1
- How Kafka topics, partitions, and consumer groups work
- How Schema Registry enforces data contracts
- Why Avro is preferred over JSON for streaming (schema enforcement, compact encoding)
- Dead letter queue pattern for fault-tolerant pipelines
- How to think about producer throughput (TPS, batching, flush intervals)

---

---

# PHASE 2 — Streaming Pipeline & Lakehouse

**Duration:** 2 weeks | **Goal:** Kafka → Spark → Delta Lake Bronze on MinIO, with quality checks

## What You're Building
```
[Kafka: transactions.raw]
        ↓ (Spark Structured Streaming)
[Quality Checks] → [DLQ if failed]
        ↓ (passed)
[Delta Lake Bronze] on [MinIO]
  /bronze/transactions/event_date=YYYY-MM-DD/
```

## Key Concepts This Phase
- **Micro-batch vs Continuous:** Spark uses micro-batches (triggers every N seconds); understand the latency/throughput trade-off
- **Watermarking:** Handles late-arriving events using event_time, not processing_time
- **Checkpointing:** Stores Kafka offsets + state in MinIO; enables crash recovery with no data loss
- **Exactly-once:** Idempotent Kafka + ACID Delta writes = no duplicates even on retry
- **Delta Lake ACID:** Atomic commits mean partial writes never appear to readers

## File Targets

### `phase2-streaming/spark_jobs/quality_checks.py`
```python
"""Quality gate: validate transaction fields, return (valid_df, invalid_df)"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

RULES = {
    "amount_positive":    col("amount") > 0,
    "user_id_not_null":   col("user_id").isNotNull(),
    "tx_id_not_null":     col("transaction_id").isNotNull(),
    "valid_event_time":   col("event_time").isNotNull() & (col("event_time") > 0),
}

def split_on_quality(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (valid_df, invalid_df). Invalid rows get a failed_rule column."""
    combined_check = None
    for rule in RULES.values():
        combined_check = rule if combined_check is None else combined_check & rule

    valid_df   = df.filter(combined_check)
    invalid_df = df.filter(~combined_check)
    return valid_df, invalid_df
```

### `phase2-streaming/spark_jobs/bronze_writer.py`
```python
"""
Kafka → Delta Lake Bronze writer
Reads transactions.raw, quality checks, writes to MinIO Delta table.
Bad records → transactions.dlq
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
from quality_checks import split_on_quality

BOOTSTRAP_SERVERS  = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC_RAW          = os.environ["KAFKA_TOPIC_RAW"]
TOPIC_DLQ          = os.environ["KAFKA_TOPIC_DLQ"]
BRONZE_PATH        = os.environ["BRONZE_PATH"]
CHECKPOINT_PATH    = os.environ["SPARK_CHECKPOINT_PATH"]
MINIO_ENDPOINT     = os.environ["MINIO_ENDPOINT"]
MINIO_USER         = os.environ["MINIO_ROOT_USER"]
MINIO_PASS         = os.environ["MINIO_ROOT_PASSWORD"]

TX_SCHEMA = StructType([
    StructField("transaction_id",  StringType()),
    StructField("user_id",         StringType()),
    StructField("amount",          DoubleType()),
    StructField("currency",        StringType()),
    StructField("merchant_id",     StringType()),
    StructField("merchant_name",   StringType()),
    StructField("country",         StringType()),
    StructField("city",            StringType()),
    StructField("device_id",       StringType()),
    StructField("device_type",     StringType()),
    StructField("event_time",      LongType()),
    StructField("is_international",BooleanType()),
])

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze-writer")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

def read_kafka(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10_000)    # backpressure control
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), TX_SCHEMA).alias("tx"), col("timestamp").alias("kafka_ts"))
        .select("tx.*", "kafka_ts")
        .withWatermark("kafka_ts", "10 minutes")   # late event window
    )

def write_bronze(df, batch_id: int):
    valid_df, invalid_df = split_on_quality(df)

    (
        valid_df
        .withColumn("loaded_at", current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .partitionBy("event_date")
        .option("mergeSchema", "true")
        .save(f"{BRONZE_PATH}/transactions")
    )

    if invalid_df.count() > 0:
        (
            invalid_df
            .selectExpr("CAST(transaction_id AS STRING) AS key", "to_json(struct(*)) AS value")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
            .option("topic", TOPIC_DLQ)
            .save()
        )

def main():
    spark = build_spark()
    df = read_kafka(spark)

    query = (
        df.writeStream
        .foreachBatch(write_bronze)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/bronze")
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()

if __name__ == "__main__":
    main()
```

## Phase 2 Validation Checklist
```
□ MinIO UI at localhost:9001 shows bucket: fraud-platform
□ Delta table exists at s3a://fraud-platform/bronze/transactions/
□ Partitions visible: event_date=YYYY-MM-DD/
□ Spark UI at localhost:8090 shows active streaming query
□ Invalid rows visible in transactions.dlq topic (Kafka UI)
□ Kill spark-streaming container → restart → no data gaps (checkpoint recovery)
□ Run: SELECT COUNT(*) FROM delta.`s3a://fraud-platform/bronze/transactions`
```

## What You'll Know After Phase 2
- Spark Structured Streaming micro-batch model and trigger intervals
- Watermarking: what it is, how to tune the threshold, what happens to late events
- Checkpointing: how Spark tracks Kafka offsets and recovers from failure
- Delta Lake ACID transactions: why partial writes never appear to readers
- Medallion architecture: why raw Bronze data is preserved unmodified
- Backpressure: maxOffsetsPerTrigger prevents memory overflow on traffic spikes

---

---

# PHASE 3 — Analytics Transformation Layer

**Duration:** 2 weeks | **Goal:** dbt transforms Bronze → Silver → Gold. Airflow orchestrates. Trino enables SQL queries.

## What You're Building
```
[Bronze Delta: raw transactions]
        ↓ (dbt + Trino)
[Silver: cleaned, enriched, deduplicated]
        ↓ (dbt)
[Gold: fraud_features, hourly_metrics, user_risk_profiles]
        ↓
[Airflow DAG: schedules everything]
```

## Key Concepts This Phase
- **Idempotent transforms:** Running dbt twice produces the same result (incremental models)
- **Data testing:** Tests are not optional — they're the contract that Silver/Gold is reliable
- **Semantic layer:** Define metrics once in dbt; every downstream tool uses the same definition
- **Orchestration:** Airflow DAG dependencies ensure Bronze is ready before dbt Silver runs

## dbt Project Structure
```
dbt_project/
├── models/
│   ├── staging/
│   │   └── stg_transactions.sql        ← Bronze source, type casts only
│   ├── intermediate/
│   │   ├── int_transactions_enriched.sql   ← Join merchant data, add flags
│   │   └── int_user_velocity.sql           ← Rolling window aggregations
│   └── marts/
│       ├── mart_fraud_features.sql         ← ML feature table
│       ├── mart_hourly_metrics.sql         ← Dashboard aggregates
│       └── mart_user_risk_profiles.sql     ← User risk scores
├── seeds/
│   └── merchants.csv                   ← Reference data
├── tests/
│   ├── assert_fraud_rate_reasonable.sql
│   └── assert_no_future_events.sql
└── macros/
    └── rolling_window.sql
```

## Key File Targets

### `models/staging/stg_transactions.sql`
```sql
-- Minimal staging: type casts, renames, source filter only.
-- No business logic here.

with source as (
    select * from {{ source('bronze', 'transactions') }}
),

typed as (
    select
        transaction_id,
        user_id,
        cast(amount as decimal(18,2))       as amount,
        upper(currency)                     as currency,
        merchant_id,
        lower(trim(merchant_name))          as merchant_name,
        upper(country)                      as country,
        city,
        device_id,
        device_type,
        timestamp_millis(event_time)        as event_at,
        cast(is_international as boolean)   as is_international,
        date(timestamp_millis(event_time))  as event_date,
        loaded_at
    from source
    where transaction_id is not null
      and amount > 0
)

select * from typed
```

### `models/intermediate/int_transactions_enriched.sql`
```sql
-- Silver: join merchant reference data, add risk flags

with txs as (
    select * from {{ ref('stg_transactions') }}
),

merchants as (
    select * from {{ ref('merchants') }}         -- seed file
),

enriched as (
    select
        t.*,
        coalesce(m.category, 'unknown')          as merchant_category,
        coalesce(m.risk_tier, 'medium')          as merchant_risk_tier,
        case
            when t.amount > 2000              then 'high'
            when t.amount > 500               then 'medium'
            else                                   'low'
        end                                      as amount_risk_tier,
        case
            when t.is_international
             and t.amount > 500              then true
            else                                  false
        end                                      as is_high_risk_international
    from txs t
    left join merchants m on t.merchant_id = m.merchant_id
)

select * from enriched
```

### `models/marts/mart_fraud_features.sql`
```sql
-- Gold: user-level features for ML model training and serving.
-- Materialized as table, refreshed every 15 min via Airflow.

{{
    config(
        materialized='incremental',
        unique_key='user_id',
        on_schema_change='sync_all_columns'
    )
}}

with enriched as (
    select * from {{ ref('int_transactions_enriched') }}
    {% if is_incremental() %}
    where event_at > (select max(feature_updated_at) from {{ this }})
    {% endif %}
),

features as (
    select
        user_id,
        count(*)                                    as tx_count_30d,
        avg(amount)                                 as avg_amount_30d,
        stddev(amount)                              as stddev_amount_30d,
        max(amount)                                 as max_amount_30d,
        count(distinct device_id)                   as unique_devices_30d,
        count(distinct country)                     as unique_countries_30d,
        sum(case when is_international then 1 else 0 end) as intl_tx_count_30d,
        sum(case when amount_risk_tier = 'high' then 1 else 0 end) as high_amount_count_30d,
        max(event_at)                               as last_tx_at,
        current_timestamp                           as feature_updated_at
    from enriched
    where event_at >= current_timestamp - interval 30 days
    group by user_id
)

select * from features
```

### `tests/assert_fraud_rate_reasonable.sql`
```sql
-- Fail if fraud_rate in any hour exceeds 50% (data quality guard)
select
    hour_bucket,
    fraud_rate
from {{ ref('mart_hourly_metrics') }}
where fraud_rate > 0.50
```

### `airflow/dags/fraud_pipeline.py`
```python
"""
Daily fraud platform pipeline.
Order: bronze_check → dbt_silver → dbt_gold → ml_features_to_redis
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner": "analytics-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="fraud_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/15 * * * *",   # every 15 min
    catchup=False,
    tags=["fraud", "dbt"],
) as dag:

    check_bronze_freshness = BashOperator(
        task_id="check_bronze_freshness",
        bash_command="""
            python /opt/airflow/scripts/check_freshness.py \
                --table bronze.transactions \
                --max-age-seconds 300
        """,
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="dbt run --select staging --profiles-dir /opt/dbt --project-dir /opt/dbt",
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_intermediate",
        bash_command="dbt run --select intermediate --profiles-dir /opt/dbt --project-dir /opt/dbt",
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command="dbt run --select marts --profiles-dir /opt/dbt --project-dir /opt/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --profiles-dir /opt/dbt --project-dir /opt/dbt",
    )

    # DAG dependency chain
    check_bronze_freshness >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test
```

## Phase 3 Validation Checklist
```
□ Trino at localhost:8083 can query: SELECT * FROM delta.bronze.transactions LIMIT 10
□ dbt run completes with 0 errors across all models
□ dbt test passes (check: dbt test output shows 0 failures)
□ Airflow UI at localhost:8082 shows fraud_pipeline DAG running on schedule
□ mart_fraud_features table has 1 row per user_id (unique key working)
□ mart_hourly_metrics has data for last 24 hours
□ Run custom test: assert_fraud_rate_reasonable returns 0 rows
```

## What You'll Know After Phase 3
- dbt model materializations: view, table, incremental — when to use each
- Schema tests vs custom SQL tests — what they catch and what they miss
- Incremental models: how `is_incremental()` and `unique_key` work together
- Airflow DAG structure: task dependencies, retries, SLA monitoring
- Trino as a query federation layer: why it matters for querying Delta from BI tools
- Semantic layer thinking: defining metrics as code, not in dashboards

---

---

# PHASE 4 — ML Deployment

**Duration:** 2 weeks | **Goal:** Feature engineering → MLflow training → model registry → FastAPI serving → prediction logging

## What You're Building
```
[Gold: mart_fraud_features]
        ↓ (feature engineering)
[Redis Feature Store]       [MLflow: train + track]
        ↓                           ↓
[FastAPI /predict] ← [MLflow Model Registry: Production]
        ↓
[Delta Lake: predictions table]
        ↓ (feedback loop)
[Airflow: retrain DAG weekly]
```

## Key Concepts This Phase
- **Train-serve skew:** Feature code used in training MUST be identical to serving
- **Model registry promotion:** Staging → Production requires AUC > 0.90 threshold
- **Shadow mode:** New model scores requests silently before promotion
- **Prediction logging:** Every prediction stored for retraining and auditing
- **Circuit breaker:** Redis down → fallback to pre-computed Delta score

## File Targets

### `phase4-ml/training/features.py`
```python
"""
Feature engineering: shared between training and serving.
NEVER duplicate this logic. Import it in both contexts.
"""
from dataclasses import dataclass
import numpy as np

@dataclass
class UserFeatures:
    tx_count_30d:        float
    avg_amount_30d:      float
    stddev_amount_30d:   float
    max_amount_30d:      float
    unique_devices_30d:  float
    unique_countries_30d: float
    intl_tx_count_30d:   float
    high_amount_count_30d: float

    def to_array(self) -> np.ndarray:
        return np.array([
            self.tx_count_30d,
            self.avg_amount_30d,
            self.stddev_amount_30d or 0.0,   # null → 0 for new users
            self.max_amount_30d,
            self.unique_devices_30d,
            self.unique_countries_30d,
            self.intl_tx_count_30d / max(self.tx_count_30d, 1),   # ratio
            self.high_amount_count_30d / max(self.tx_count_30d, 1), # ratio
        ]).reshape(1, -1)

FEATURE_NAMES = [
    "tx_count_30d",
    "avg_amount_30d",
    "stddev_amount_30d",
    "max_amount_30d",
    "unique_devices_30d",
    "unique_countries_30d",
    "intl_tx_ratio_30d",
    "high_amount_ratio_30d",
]
```

### `phase4-ml/training/train.py`
```python
"""
Model training pipeline.
Pulls features from Gold table, trains RandomForest, logs to MLflow.
Promotes to Staging if AUC > threshold.
"""
import os
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, precision_score, recall_score
from features import FEATURE_NAMES

TRACKING_URI   = os.environ["MLFLOW_TRACKING_URI"]
EXPERIMENT     = os.environ["MLFLOW_EXPERIMENT_NAME"]
MODEL_NAME     = os.environ["MODEL_NAME"]
MIN_AUC        = float(os.environ.get("MIN_AUC_FOR_PROMOTION", 0.90))
TRINO_HOST     = os.environ.get("TRINO_HOST", "trino")

mlflow.set_tracking_uri(TRACKING_URI)
mlflow.set_experiment(EXPERIMENT)

def load_training_data() -> pd.DataFrame:
    from trino.dbapi import connect
    conn = connect(host=TRINO_HOST, port=8083, user="training", catalog="delta", schema="gold")
    cur = conn.cursor()
    cur.execute("""
        SELECT f.*, COALESCE(l.is_fraud, false) as label
        FROM fraud_features f
        LEFT JOIN fraud_labels l USING (user_id)
    """)
    cols = [desc[0] for desc in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)

def train(df: pd.DataFrame):
    X = df[FEATURE_NAMES].fillna(0)
    y = df["label"].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    params = {"n_estimators": 200, "max_depth": 8, "class_weight": "balanced", "random_state": 42}

    with mlflow.start_run() as run:
        model = RandomForestClassifier(**params)
        model.fit(X_train, y_train)

        y_prob = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, y_prob)

        mlflow.log_params(params)
        mlflow.log_metrics({
            "auc_roc":   auc,
            "precision": precision_score(y_test, y_prob > 0.5),
            "recall":    recall_score(y_test, y_prob > 0.5),
            "train_size": len(X_train),
        })
        mlflow.sklearn.log_model(model, "model", registered_model_name=MODEL_NAME)

        if auc >= MIN_AUC:
            promote_to_staging(run.info.run_id)
            print(f"Promoted to Staging: AUC={auc:.4f}")
        else:
            print(f"Not promoted: AUC={auc:.4f} < threshold={MIN_AUC}")

def promote_to_staging(run_id: str):
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='{MODEL_NAME}' and run_id='{run_id}'")
    if versions:
        client.transition_model_version_stage(MODEL_NAME, versions[0].version, "Staging")

if __name__ == "__main__":
    df = load_training_data()
    train(df)
```

### `phase4-ml/serving/main.py`
```python
"""FastAPI fraud prediction service"""
import os
import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from predictor import Predictor
from feature_client import FeatureClient

app = FastAPI(title="Fraud Detection API", version="1.0")

predictor    = Predictor()
feature_client = FeatureClient()

class PredictRequest(BaseModel):
    transaction_id: str
    user_id:        str
    amount:         float
    merchant_id:    str
    is_international: bool

class PredictResponse(BaseModel):
    transaction_id:   str
    fraud_probability: float
    risk_tier:        str          # low / medium / high
    model_version:    str
    latency_ms:       float

@app.get("/health")
def health():
    return {"status": "ok", "model_version": predictor.model_version}

@app.post("/predict", response_model=PredictResponse)
def predict(req: PredictRequest):
    import time
    start = time.monotonic()

    features = feature_client.get(req.user_id)
    if features is None:
        features = feature_client.get_fallback(req.user_id)

    prob = predictor.score(features)
    risk = "high" if prob > 0.7 else "medium" if prob > 0.4 else "low"
    latency_ms = (time.monotonic() - start) * 1000

    predictor.log_prediction(req.transaction_id, req.user_id, prob, latency_ms)

    return PredictResponse(
        transaction_id=req.transaction_id,
        fraud_probability=round(prob, 4),
        risk_tier=risk,
        model_version=predictor.model_version,
        latency_ms=round(latency_ms, 2),
    )
```

### `phase4-ml/serving/feature_client.py`
```python
"""Redis feature store client with Delta Lake fallback"""
import os, json, redis
from features import UserFeatures

REDIS_HOST = os.environ["REDIS_HOST"]
REDIS_PORT = int(os.environ["REDIS_PORT"])
KEY_PREFIX = "user_features:"

_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def get(user_id: str) -> UserFeatures | None:
    try:
        raw = _redis.get(f"{KEY_PREFIX}{user_id}")
        if raw:
            return UserFeatures(**json.loads(raw))
    except redis.RedisError:
        pass
    return None

def get_fallback(user_id: str) -> UserFeatures:
    # Return conservative defaults if no feature data exists
    return UserFeatures(
        tx_count_30d=0, avg_amount_30d=0, stddev_amount_30d=0,
        max_amount_30d=0, unique_devices_30d=1, unique_countries_30d=1,
        intl_tx_count_30d=0, high_amount_count_30d=0,
    )

def set_features(user_id: str, features: UserFeatures, ttl: int = 86400):
    try:
        _redis.setex(f"{KEY_PREFIX}{user_id}", ttl, json.dumps(features.__dict__))
    except redis.RedisError as e:
        pass  # non-fatal: stale features in Redis is acceptable
```

## Phase 4 Validation Checklist
```
□ MLflow UI at localhost:5000 shows experiment with logged runs
□ Model registered under fraud_classifier in Model Registry
□ At least 1 model version promoted to Staging (AUC gate passed)
□ curl -X POST localhost:8000/predict -H "Content-Type: application/json" \
     -d '{"transaction_id":"t1","user_id":"U1234","amount":500,"merchant_id":"M001","is_international":false}'
  → returns fraud_probability + model_version
□ Redis contains feature keys: redis-cli keys "user_features:*"
□ Prediction logged to Delta Lake predictions table
□ Kill Redis → /predict still returns response (fallback working)
```

## What You'll Know After Phase 4
- MLflow experiment tracking: what to log and why (params, metrics, artifacts)
- Model registry lifecycle: None → Staging → Production → Archived
- Train-serve skew: why sharing feature code is non-negotiable
- Circuit breaker pattern: Redis fallback prevents API outage from cache failure
- Prediction logging: foundation for the retraining feedback loop
- FastAPI async serving: how to structure a production ML endpoint

---

---

# PHASE 5 — Analytics Layer & Production Hardening

**Duration:** 2 weeks | **Goal:** Dashboards live, observability complete, system is production-hardened

## What You're Building
```
Grafana ← Prometheus ← [all services exporting /metrics]
                 ↑ pipeline health, Kafka lag, Spark, ML monitoring

Superset ← Trino ← Gold Delta Tables
                 ↑ business analytics, self-serve SQL
```

## Key Concepts This Phase
- **Four golden signals:** Latency, traffic, errors, saturation — instrument all of them
- **Alerting philosophy:** Alert on symptoms (SLA breach), not causes (high CPU)
- **Self-serve analytics:** Superset semantic layer means analysts write SQL, not request changes
- **Drift detection:** Model input distribution shift → retrain trigger

## Grafana Dashboard: Pipeline Health
Panels to build (connect to Prometheus datasource):
```
Row 1 — Ingestion
  • Kafka messages/sec (per topic)
  • Kafka consumer lag (per partition)
  • DLQ message count (alert if > 0 in 5 min)

Row 2 — Processing
  • Spark streaming batch duration (alert if P95 > 30s)
  • Spark records processed/sec
  • Bronze table freshness (time since last write)

Row 3 — Transforms
  • dbt run duration (per model)
  • dbt test failures (alert if > 0)
  • Silver/Gold table freshness
```

## Grafana Dashboard: ML Monitoring
Panels to build:
```
Row 1 — Serving
  • Prediction requests/min
  • API latency P50 / P95 / P99
  • Error rate %

Row 2 — Model Health
  • Fraud rate (7-day rolling) — alert if > 2x baseline
  • Feature drift score (PSI per feature) — alert if PSI > 0.2
  • Model version currently serving

Row 3 — Business
  • High-risk predictions / hour
  • Prediction distribution (histogram of fraud_probability)
```

## Superset Setup
Dashboards to build (connect via Trino):
```
1. Transaction Overview
   • Volume by hour (line chart)
   • Amount distribution (histogram)
   • Top merchants by volume (bar chart)

2. Fraud Analytics
   • Fraud rate by country (choropleth)
   • Merchant risk ranking (table)
   • High-risk user cohort (scatter: tx_count vs avg_amount)

3. Real-Time Risk Feed
   • Last 100 high-risk predictions (live table)
   • Risk tier distribution (pie)
```

## Production Hardening — docker-compose.yml additions
```yaml
# Add to ALL stateful services:
restart: unless-stopped
deploy:
  resources:
    limits:
      memory: 2g
      cpus: "1.5"

# Add to ALL services:
healthcheck:
  test: [...]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 60s

# Use secrets, not env vars for credentials:
secrets:
  postgres_password:
    file: ./secrets/postgres_password.txt
```

## Phase 5 Validation Checklist
```
□ Grafana at localhost:3000 shows both dashboards with live data
□ Prometheus at localhost:9090 scrapes all targets (Status > Targets: all UP)
□ Superset at localhost:8088 shows 3 dashboards with real data
□ Trigger DLQ message → Grafana alert fires within 2 minutes
□ Simulate drift: send all transactions with amount > 4000 for 10 min
  → PSI score rises in ML Monitoring dashboard
□ All docker compose ps shows: healthy (not starting / unhealthy)
□ make test passes end-to-end
```

## What You'll Know After Phase 5
- Prometheus scrape model: pull vs push, labels, metric types (counter, gauge, histogram)
- Grafana: datasources, panels, variables, alert rules
- Superset: database connections, datasets, charts, dashboards, semantic layer
- What to alert on vs what to monitor (alert on SLA breach, not resource usage)
- Model drift: PSI vs KS test, when to retrain vs when to investigate
- Production Docker Compose: health checks, restart policies, resource limits

---

---

# Phase Progression Summary

| Phase | Weeks | Services Added | Core Skill |
|---|---|---|---|
| 1 — Ingestion | 1–2 | Kafka, Schema Registry, Producer | Data contracts, streaming fundamentals |
| 2 — Streaming | 3–4 | Spark, MinIO, Delta Lake | Real-time pipelines, ACID storage |
| 3 — Transforms | 5–6 | dbt, Airflow, Trino | Analytics layer design, orchestration |
| 4 — ML | 7–8 | MLflow, Redis, FastAPI | End-to-end ML lifecycle |
| 5 — Analytics | 9–10 | Grafana, Prometheus, Superset | Observability, self-serve BI |

---

# Prompt Templates for Each Phase

Use these as your opening prompt in each Claude Code session.

## Phase 1 Prompt
```
I'm building a production-grade fraud detection analytics platform in phases.
We're starting Phase 1: Data Ingestion Infrastructure.

Stack: Kafka + Schema Registry + Python producer, all on Docker Compose.
Repo: fraud-platform/

Goals this session:
1. Set up the full directory structure per CLAUDE.md
2. Write the docker-compose.yml for Phase 1 services
3. Write the Avro schema and Python producer per the specs in CLAUDE.md
4. Write a Makefile with phase1-up, down, logs targets
5. Write pytest tests for the producer
6. Write a .env.example with all Phase 1 variables

Coding rules (from CLAUDE.md):
- No verbose comments, clean self-documenting code
- Config via env vars only
- Structured JSON logging

Start by creating the directory structure, then work through each file.
When done, walk me through: make phase1-up and how to verify it's working.
```

## Phase 2 Prompt
```
Continuing the fraud platform. Phase 1 is complete and running.
Now building Phase 2: Streaming Pipeline & Lakehouse.

Phase 1 is running: Kafka on kafka:29092, Schema Registry on 8081.
New goal: Kafka → Spark Structured Streaming → Delta Lake on MinIO.

Goals this session:
1. Add MinIO + Spark services to docker-compose.yml
2. Write bronze_writer.py and quality_checks.py per CLAUDE.md specs
3. Write the MinIO bucket init script
4. Write Spark Dockerfile with Delta Lake + S3A dependencies
5. Write pytest tests for quality_checks.py
6. Extend Makefile with phase2-up

Key requirements:
- Watermarking on event_time (10 min window)
- Checkpointing to MinIO for crash recovery
- maxOffsetsPerTrigger=10000 for backpressure
- Invalid rows → transactions.dlq (not dropped)

When done, show me how to verify Delta partitions are writing to MinIO
and how to test checkpoint recovery (kill + restart the container).
```

## Phase 3 Prompt
```
Continuing the fraud platform. Phases 1 and 2 are running.
Bronze Delta table is writing to MinIO. Now building Phase 3: Analytics Layer.

Goals this session:
1. Add Postgres + Airflow + Trino + dbt services to docker-compose.yml
2. Scaffold the full dbt project structure per CLAUDE.md
3. Write stg_transactions.sql, int_transactions_enriched.sql, mart_fraud_features.sql
4. Write dbt tests: schema tests + 2 custom SQL tests
5. Write the Airflow fraud_pipeline DAG per CLAUDE.md
6. Configure Trino delta catalog to point at MinIO

When done, show me:
- How to verify Trino can query the Bronze table
- How to run dbt manually and check test results
- How the Airflow DAG dependency chain works
```

## Phase 4 Prompt
```
Continuing the fraud platform. Phases 1–3 are running.
Gold tables are populated. Now building Phase 4: ML Deployment.

Goals this session:
1. Add MLflow + Redis + FastAPI services to docker-compose.yml
2. Write features.py (shared between training and serving — do not duplicate)
3. Write train.py with MLflow logging and Staging promotion gate
4. Write main.py (FastAPI), predictor.py, feature_client.py with Redis fallback
5. Write the Redis feature writer (triggered by Airflow after dbt Gold run)
6. Write pytest tests for feature_client.py (mock Redis failure)
7. Add ml_training DAG to Airflow

Key requirement:
- features.py must be importable by both training/ and serving/
- Redis failure must NOT crash the API (fallback to defaults)
- Every prediction must be logged to Delta Lake

When done, show me how to:
- Run a training job and see the result in MLflow UI
- Promote a model from Staging to Production
- Call /predict and verify the prediction is logged
```

## Phase 5 Prompt
```
Continuing the fraud platform. Phases 1–4 are running.
Fraud API is serving predictions. Now building Phase 5: Analytics & Hardening.

Goals this session:
1. Add Prometheus + Grafana + Superset to docker-compose.yml
2. Add /metrics endpoint to the FastAPI app (prometheus_client)
3. Configure Prometheus scrape targets for all services
4. Build Grafana pipeline health dashboard (Kafka lag, Spark metrics, freshness)
5. Build Grafana ML monitoring dashboard (prediction volume, drift score, latency)
6. Configure Superset connection to Trino, create 3 datasets from Gold tables
7. Add health checks and resource limits to all docker-compose services
8. Write a final make test that runs all phase tests end-to-end

When done, show me:
- How to trigger a Grafana alert (simulate DLQ messages)
- How to verify all services show 'healthy' in docker compose ps
- How to run the full test suite with make test
```