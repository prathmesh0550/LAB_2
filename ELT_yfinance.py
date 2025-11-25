from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

DBT_PROJECT_DIR = "/opt/airflow/yfinance_elt"

conn = BaseHook.get_connection("snowflake_conn")

default_env = {
    "PATH": "/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin",
    "DBT_USER": conn.login,
    "DBT_PASSWORD": conn.password,
    "DBT_ACCOUNT": conn.extra_dejson.get("account"),
    "DBT_SCHEMA": conn.schema,
    "DBT_DATABASE": conn.extra_dejson.get("database"),
    "DBT_ROLE": conn.extra_dejson.get("role"),
    "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
}

with DAG(
    dag_id="ELT_yfinance_dbt",
    start_date=datetime(2025, 11, 23),
    schedule=None,
    catchup=False,
    tags=["yfinance", "dbt", "ELT"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env=default_env,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env=default_env,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
        env=default_env,
    )

    dbt_run >> dbt_test >> dbt_snapshot
