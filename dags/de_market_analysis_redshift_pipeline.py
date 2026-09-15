from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

def alert_on_failure(context):
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    execution_date = context["execution_date"]
    print(f"ALERT: Task '{task_id}' in DAG '{dag_id}' failed at {execution_date}")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="de_market_analysis_redshift_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_laus_s3 = BashOperator(
        task_id="extract_laus_s3",
        bash_command="cd /opt/de-market-analysis && python extract/extract_laus_s3.py"
        # retries=0, # overrides default_args for just this task
    )

    truncate_laus = SQLExecuteQueryOperator(
        task_id="truncate_laus",
        conn_id="redshift_default",
        sql="TRUNCATE TABLE raw_laus_data;"
    )

    copy_laus_redshift = SQLExecuteQueryOperator(
        task_id="copy_laus_redshift",
        conn_id="redshift_default",
        sql="""
            COPY raw_laus_data
            FROM 's3://s3-learn-bucket-381492047455-us-west-2-an/raw/laus/laus_data.csv'
            IAM_ROLE 'arn:aws:iam::381492047455:role/aws-learn-redshift'
            CSV
            IGNOREHEADER 1;
        """
    )

    extract_oews_s3 = BashOperator(
        task_id="extract_oews_s3",
        bash_command="cd /opt/de-market-analysis && python extract/extract_oews_s3.py"
    )

    truncate_oews = SQLExecuteQueryOperator(
        task_id="truncate_oews",
        conn_id="redshift_default",
        sql="TRUNCATE TABLE raw_oews_data;"
    )

    copy_oews_redshift = SQLExecuteQueryOperator(
        task_id="copy_oews_redshift",
        conn_id="redshift_default",
        sql="""
            COPY raw_oews_data
            FROM 's3://s3-learn-bucket-381492047455-us-west-2-an/raw/oews/oews_data.csv'
            IAM_ROLE 'arn:aws:iam::381492047455:role/aws-learn-redshift'
            CSV
            IGNOREHEADER 1;
        """
    )

    dbt_run_redshift = BashOperator(
        task_id="dbt_run_redshift",
        bash_command="cd /opt/de-market-analysis && dbt run --target redshift --select stg_laus_data stg_oews_data"
    )

    extract_laus_s3 >> truncate_laus >> copy_laus_redshift
    extract_oews_s3 >> truncate_oews >> copy_oews_redshift

    [copy_laus_redshift, copy_oews_redshift] >> dbt_run_redshift