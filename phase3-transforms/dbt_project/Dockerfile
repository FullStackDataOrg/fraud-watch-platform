FROM python:3.11-slim

RUN pip install --no-cache-dir \
    dbt-trino==1.7.1 \
    trino==0.327.0

WORKDIR /opt/dbt
COPY . .

# Default: run the full pipeline once and exit.
CMD ["dbt", "run", "--profiles-dir", "/opt/dbt", "--project-dir", "/opt/dbt"]
