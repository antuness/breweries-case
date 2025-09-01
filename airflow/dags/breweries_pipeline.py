from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os, requests

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL")

def notify_slack(context, title="Task Failure"):
    if not SLACK_WEBHOOK: return
    ti = context["task_instance"]
    payload = {
        "text": (f"*{title}* :rotating_light:\n"
                 f"DAG: `{ti.dag_id}` | Task: `{ti.task_id}`\n"
                 f"When: `{context.get('execution_date')}`\n"
                 f"Logs: {ti.log_url}\n"
                 f"Error: ```{str(context.get('exception'))[:350]}```")
    }
    try: requests.post(SLACK_WEBHOOK, json=payload, timeout=5)
    except Exception: pass

default_args = {
    "owner": "samira",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack,
}

with DAG(
    dag_id="breweries_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    sla_miss_callback=lambda **ctx: notify_slack(ctx, title="SLA Miss"),
    tags=["case","breweries"],
) as dag:

    ingest = BashOperator(
        task_id="ingest",
        bash_command="python /opt/airflow/app/ingestion/fetch_breweries.py",
        sla=timedelta(minutes=10),
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="python /opt/airflow/app/transforms/transform_silver.py",
        sla=timedelta(minutes=10),
    )

    quality = BashOperator(
        task_id="quality_checks",
        bash_command="python /opt/airflow/app/quality/run_quality_checks.py",
        trigger_rule="all_done",
    )

    aggregate = BashOperator(
        task_id="aggregate",
        bash_command="python /opt/airflow/app/transforms/aggregate_gold.py",
    )

    ingest >> transform >> quality >> aggregate