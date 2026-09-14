
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id="de_market_analysis_redshift_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_laus_s3 = BashOperator(
        task_id="extract_laus_s3",
        bash_command="cd /opt/de-market-analysis && python extract/extract_laus_s3.py"
    )

    truncate_and_copy_redshift = SQLExecuteQueryOperator(
        task_id="copy_to_redshift",
        conn_id="redshift_default",
        sql="""
            TRUNCATE TABLE raw_laus_data;
            COPY raw_laus_data
            FROM 's3://s3-learn-bucket-381492047455-us-west-2-an/raw/laus/laus_data.csv'
            IAM_ROLE 'arn:aws:iam::381492047455:role/aws-learn-redshift'
            CSV
            IGNOREHEADER 1;
        """
    )

    dbt_run_redshift = BashOperator(
        task_id="dbt_run_redshift",
        bash_command="cd /opt/de-market-analysis && dbt run --target redshift --select stg_laus_data"
    )

    extract_laus_s3 >> truncate_and_copy_redshift >> dbt_run_redshift