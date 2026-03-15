"""
Weekly ML training DAG.
Runs training job → on success, promotes best Staging model to Production.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "ml-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="ml_training",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * 1",   # every Monday at 03:00
    catchup=False,
    tags=["fraud", "ml", "training"],
) as dag:

    run_training = BashOperator(
        task_id="run_training",
        bash_command="""
            cd /opt/dbt && python -c "
import sys
sys.path.insert(0, '/opt/ml')
from train import train
train()
"
        """,
    )

    promote_to_production = BashOperator(
        task_id="promote_to_production",
        bash_command="""
            python -c "
import os, mlflow
mlflow.set_tracking_uri(os.environ['MLFLOW_TRACKING_URI'])
client = mlflow.tracking.MlflowClient()
staging = client.get_latest_versions(os.environ['MODEL_NAME'], stages=['Staging'])
if staging:
    client.transition_model_version_stage(
        os.environ['MODEL_NAME'], staging[0].version, 'Production'
    )
    print(f'Promoted version {staging[0].version} to Production')
else:
    print('No Staging model found — skipping promotion')
"
        """,
    )

    run_training >> promote_to_production
