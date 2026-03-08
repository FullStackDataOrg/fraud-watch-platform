FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    dbt-trino==1.7.1 \
    trino==0.327.0 \
    apache-airflow-providers-trino==4.3.1

# Init DB, create admin user, then start the all-in-one server
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["airflow db migrate && \
      airflow users create \
        --username $AIRFLOW_ADMIN_USER \
        --password $AIRFLOW_ADMIN_PASSWORD \
        --firstname Admin --lastname User \
        --role Admin --email admin@fraudplatform.io && \
      airflow scheduler & \
      exec airflow webserver"]
