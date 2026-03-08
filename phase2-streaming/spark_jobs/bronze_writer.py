"""
Kafka → Delta Lake Bronze writer.
Reads transactions.raw (Avro + Schema Registry), quality checks, writes to MinIO.
Bad records → transactions.dlq
"""
import json
import os

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, expr, from_unixtime, to_date

from quality_checks import split_on_quality

BOOTSTRAP_SERVERS   = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
TOPIC_RAW           = os.environ["KAFKA_TOPIC_RAW"]
TOPIC_DLQ           = os.environ["KAFKA_TOPIC_DLQ"]
BRONZE_PATH         = os.environ["BRONZE_PATH"]
CHECKPOINT_PATH     = os.environ["SPARK_CHECKPOINT_PATH"]
MINIO_ENDPOINT      = os.environ["MINIO_ENDPOINT"]
MINIO_USER          = os.environ["MINIO_ROOT_USER"]
MINIO_PASS          = os.environ["MINIO_ROOT_PASSWORD"]


def fetch_avro_schema(topic: str) -> str:
    """
    Fetch schema from Schema Registry and strip logicalType from event_time.
    Keeps event_time as a plain Long (epoch millis) instead of Spark TimestampType,
    so quality checks and partitioning logic stay consistent.
    """
    resp = requests.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{topic}-value/versions/latest",
        timeout=10,
    )
    resp.raise_for_status()
    schema = json.loads(resp.json()["schema"])
    for field in schema["fields"]:
        if field["name"] == "event_time" and isinstance(field.get("type"), dict):
            field["type"] = {"type": "long"}
    return json.dumps(schema)


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
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession, avro_schema: str) -> DataFrame:
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10_000)
        .option("failOnDataLoss", "false")
        .load()
    )
    # Confluent wire format: [0x00 magic][4-byte schema ID][avro payload]
    # substring() is 1-indexed; byte 6 onward is the raw Avro binary payload
    return (
        raw
        .select(
            from_avro(expr("substring(value, 6)"), avro_schema).alias("tx"),
            col("timestamp").alias("kafka_ts"),
        )
        .select("tx.*", "kafka_ts")
        .withWatermark("kafka_ts", "10 minutes")
    )


def write_bronze(df: DataFrame, batch_id: int) -> None:
    valid_df, invalid_df = split_on_quality(df)

    (
        valid_df
        .withColumn("loaded_at", current_timestamp())
        .withColumn("event_date", to_date(from_unixtime(col("event_time") / 1000)))
        .write
        .format("delta")
        .mode("append")
        .partitionBy("event_date")
        .option("mergeSchema", "true")
        .save(f"{BRONZE_PATH}/transactions")
    )

    (
        invalid_df
        .selectExpr("CAST(transaction_id AS STRING) AS key", "to_json(struct(*)) AS value")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", TOPIC_DLQ)
        .save()
    )


def main() -> None:
    spark = build_spark()
    avro_schema = fetch_avro_schema(TOPIC_RAW)
    df = read_kafka(spark, avro_schema)

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
