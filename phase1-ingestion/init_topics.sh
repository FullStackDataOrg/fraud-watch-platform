#!/usr/bin/env bash
set -e

BOOTSTRAP=${KAFKA_HOST:-kafka:29092}

create_topic() {
  local name=$1
  local partitions=$2
  kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1
  echo "topic ready: $name"
}

create_topic transactions.raw      10
create_topic transactions.enriched 10
create_topic transactions.dlq       3
