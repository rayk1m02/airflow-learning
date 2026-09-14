from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="de_market_analysis_pipeline",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_laus = BashOperator(
        task_id="extract_laus",
        bash_command="cd /opt/de-market-analysis && python extract/extract_laus.py"
    )

    extract_oews = BashOperator(
        task_id="extract_oews",
        bash_command="cd /opt/de-market-analysis && python extract/extract_oews.py"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/de-market-analysis && dbt run"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/de-market-analysis && dbt test"
    )

    extract_laus >> extract_oews >> dbt_run >> dbt_test