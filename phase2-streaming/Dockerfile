FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    pip install --no-cache-dir requests==2.31.0 && \
    rm -rf /var/lib/apt/lists/*

# Bake required JARs into the image so spark-submit needs no network at runtime
ENV JARS=/opt/spark/jars

# Delta Lake
RUN curl -fsSL "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar" \
        -o "$JARS/delta-spark_2.12-3.1.0.jar" && \
    curl -fsSL "https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar" \
        -o "$JARS/delta-storage-3.1.0.jar"

# Spark ↔ Kafka connector (must match Spark version)
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar" \
        -o "$JARS/spark-sql-kafka-0-10_2.12-3.5.0.jar" && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar" \
        -o "$JARS/spark-token-provider-kafka-0-10_2.12-3.5.0.jar" && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar" \
        -o "$JARS/kafka-clients-3.5.1.jar" && \
    curl -fsSL "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
        -o "$JARS/commons-pool2-2.11.1.jar"

# Spark Avro (for from_avro / Schema Registry deserialization)
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.0/spark-avro_2.12-3.5.0.jar" \
        -o "$JARS/spark-avro_2.12-3.5.0.jar"

# S3A / MinIO access
RUN curl -fsSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
        -o "$JARS/hadoop-aws-3.3.4.jar" && \
    curl -fsSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
        -o "$JARS/aws-java-sdk-bundle-1.12.262.jar"

WORKDIR /app
COPY spark_jobs/ .
COPY tests/ tests/

ENV PATH="/opt/spark/bin:${PATH}"

# Shell form allows env-var expansion at container start.
# The apache/spark entrypoint passes unrecognised commands through via exec "$@".
# local[*] runs driver + executors in the same JVM so all baked-in JARs are available.
# spark-master/worker still run for the Spark UI; submit to them for batch jobs in later phases.
CMD spark-submit \
    --master "local[*]" \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    bronze_writer.py
