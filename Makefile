.PHONY: up down logs build ps clean test \
        phase1-up phase2-up phase3-up phase4-up phase5-up \
        phase1-test phase2-test phase3-test \
        kafka-topics kafka-lag schema-list producer-logs \
        spark-logs minio-ls bronze-count \
        trino-query trino-bronze-count airflow-logs dbt-run dbt-test

# ── Full stack ────────────────────────────────────────────
up:
	docker compose up -d

down:
	docker compose down

clean:
	docker compose down -v --remove-orphans

build:
	docker compose build

ps:
	docker compose ps

logs:
	docker compose logs -f $(filter-out $@,$(MAKECMDGOALS))

# ── Phase targets ─────────────────────────────────────────
phase1-up:
	docker compose up -d kafka schema-registry kafka-ui topic-init
	docker compose up -d transaction-producer

phase2-up:
	docker compose up -d minio
	docker compose up -d minio-init
	docker compose up -d spark-master spark-worker
	docker compose up -d spark-streaming

phase3-up:
	docker compose up -d postgres
	docker compose up -d trino
	docker compose up -d airflow
	docker compose up -d dbt-runner

phase4-up:
	docker compose up -d mlflow redis ml-training fraud-api

phase5-up:
	docker compose up -d prometheus grafana superset

# ── Tests ─────────────────────────────────────────────────
phase1-test:
	PYTHONDONTWRITEBYTECODE=1 pytest phase1-ingestion/tests/ -v

phase2-test:
	docker compose run --rm --entrypoint "" spark-streaming \
		bash -c "pip3 install pytest==7.4.0 -q && cd /app && pytest tests/ -v"

phase3-test:
	docker compose run --rm --entrypoint "" dbt-runner \
		bash -c "dbt test --profiles-dir /opt/dbt --project-dir /opt/dbt"

test: phase1-test phase2-test phase3-test

# ── Kafka helpers ─────────────────────────────────────────
kafka-topics:
	docker compose exec kafka kafka-topics \
		--bootstrap-server localhost:29092 --list

kafka-lag:
	docker compose exec kafka kafka-consumer-groups \
		--bootstrap-server localhost:29092 --describe --all-groups

kafka-describe:
	docker compose exec kafka kafka-topics \
		--bootstrap-server localhost:29092 --describe

schema-list:
	curl -s http://localhost:8081/subjects | python3 -m json.tool

producer-logs:
	docker compose logs -f transaction-producer

kafka-logs:
	docker compose logs -f kafka

# ── Phase 3 helpers ────────────────────────────────────────
airflow-logs:
	docker compose logs -f airflow

trino-query:
	docker compose exec trino trino --catalog delta --schema bronze || true

trino-bronze-count:
	docker compose exec trino trino --catalog delta --schema bronze \
		--execute "SELECT COUNT(*) FROM transactions"

dbt-run:
	docker compose run --rm --entrypoint "" dbt-runner \
		bash -c "dbt seed --profiles-dir /opt/dbt --project-dir /opt/dbt && \
		         dbt run  --profiles-dir /opt/dbt --project-dir /opt/dbt"

dbt-test:
	docker compose run --rm --entrypoint "" dbt-runner \
		dbt test --profiles-dir /opt/dbt --project-dir /opt/dbt

# ── Phase 2 helpers ────────────────────────────────────────
spark-logs:
	docker compose logs -f spark-streaming

minio-ls:
	docker compose exec minio mc ls --recursive local/fraud-platform/

bronze-count:
	docker compose exec spark-master spark-sql \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		-e "SELECT COUNT(*) FROM delta.\`${BRONZE_PATH}/transactions\`"

%:
	@: