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