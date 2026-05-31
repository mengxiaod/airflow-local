"""
My first Airflow DAG — Hello World
This DAG demonstrates BashOperator and PythonOperator
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# Default arguments applied to all tasks
default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def greet(name, **kwargs):
    """A simple Python function that greets someone"""
    execution_date = kwargs["ds"]
    print(f"Hello, {name}!")
    print(f"This task ran for execution date: {execution_date}")
    print(f"Current timestamp: {datetime.now().isoformat()}")
    return f"Greeted {name} successfully"


with DAG(
    dag_id="hello_world",
    default_args=default_args,
    description="My first Airflow DAG",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["tutorial", "hello-world"],
) as dag:

    # Task 1: Run a bash command
    print_date = BashOperator(
        task_id="print_date",
        bash_command="echo 'Current date:' && date && echo 'Airflow is running!'",
    )

    # Task 2: Run a Python function
    greet_task = PythonOperator(
        task_id="greet",
        python_callable=greet,
        op_kwargs={"name": "Airflow Developer"},
    )

    # Define dependency: print_date runs first, then greet
    print_date >> greet_task
