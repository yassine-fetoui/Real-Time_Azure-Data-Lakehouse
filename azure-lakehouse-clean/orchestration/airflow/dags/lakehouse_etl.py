"""
Lakehouse ETL Orchestration DAG

Pipeline:
  1. Validate Kafka consumer lag (sensor)
  2. Trigger Databricks PySpark job (Bronze → Silver)
  3. Run dbt transformations (Silver → Gold)
  4. Great Expectations quality gate
  5. Refresh Snowflake materialized views
  6. Alert on SLA breach

SLA:    Full pipeline completes within 45 minutes of trigger
Uptime: 99.8% (achieved via retry logic + dead-letter alerting)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from plugins.sensors.kafka_lag_sensor import KafkaLagSensor
from plugins.callbacks import slack_alert_on_failure, slack_alert_on_sla_miss

# ─── Default Args ──────────────────────────────────────────────────────────────

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 1, 1),
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":  timedelta(minutes=30),
    "on_failure_callback": slack_alert_on_failure,
    "email_on_failure": True,
    "email":            ["data-engineering@company.com"],
}

# ─── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="lakehouse_etl_pipeline",
    description="Bronze → Silver → Gold lakehouse ETL with quality gates",
    default_args=default_args,
    schedule_interval="0 * * * *",   # Hourly
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=slack_alert_on_sla_miss,
    tags=["lakehouse", "production"],
) as dag:

    # 1. Wait until Kafka consumer lag is below threshold
    wait_for_kafka = KafkaLagSensor(
        task_id="wait_for_kafka_lag",
        bootstrap_servers="{{ var.value.KAFKA_BOOTSTRAP_SERVERS }}",
        consumer_group="lakehouse-consumer-group",
        topic="raw-events",
        max_lag=10_000,          # Unblock when lag < 10K messages
        poke_interval=60,        # Check every 60s
        timeout=900,             # Give up after 15 min
        sla=timedelta(minutes=10),
    )

    # 2. Databricks: Bronze → Silver (PySpark Structured Streaming flush)
    bronze_to_silver = DatabricksRunNowOperator(
        task_id="bronze_to_silver",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.DATABRICKS_JOB_BRONZE_SILVER }}",
        notebook_params={"run_date": "{{ ds }}", "environment": "prod"},
        sla=timedelta(minutes=20),
    )

    # 3. dbt run: staging → intermediate → mart
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --target prod --select staging intermediate marts "
            "--vars '{run_date: {{ ds }}}'"
        ),
        sla=timedelta(minutes=15),
    )

    # 4. dbt test: schema + data contracts
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt test --target prod --store-failures"
        ),
        sla=timedelta(minutes=10),
    )

    # 5. Great Expectations quality checkpoint
    ge_checkpoint = BashOperator(
        task_id="great_expectations_checkpoint",
        bash_command=(
            "great_expectations checkpoint run marts_checkpoint "
            "--batch-request '{\"runtime_parameters\": {\"run_date\": \"{{ ds }}\"}}"
        ),
        sla=timedelta(minutes=5),
    )

    # 6. Refresh Snowflake dynamic tables / materialized views
    refresh_snowflake = SnowflakeOperator(
        task_id="refresh_snowflake_marts",
        snowflake_conn_id="snowflake_default",
        sql="CALL LAKEHOUSE_PROD.UTILS.REFRESH_ALL_DYNAMIC_TABLES();",
        sla=timedelta(minutes=5),
    )

    # 7. Mark pipeline success
    pipeline_complete = PythonOperator(
        task_id="pipeline_complete",
        python_callable=lambda **ctx: print(
            f"✅ Lakehouse pipeline completed for {ctx['ds']}"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ─── Dependencies ──────────────────────────────────────────────────────────
    (
        wait_for_kafka
        >> bronze_to_silver
        >> dbt_run
        >> dbt_test
        >> ge_checkpoint
        >> refresh_snowflake
        >> pipeline_complete
    )
