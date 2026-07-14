from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def hello():
    print("Hello World!")


with DAG(
    dag_id="hello_world",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["test"],
) as dag:

    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=hello,
    )