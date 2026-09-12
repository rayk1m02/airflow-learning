from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_message():
    print("This task ran via PythonOperator")

with DAG(
    dag_id="hello_world",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
) as dag:

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Airflow'"
    )

    say_python = PythonOperator(
        task_id="say_python",
        python_callable=print_message
    )

    say_hello >> say_python # dependency syntax (run say_hello before say_python