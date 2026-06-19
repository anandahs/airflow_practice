from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

with DAG(dag_id="context_dag", schedule=None, start_date=datetime(2026, 6, 19), catchup=False) as dag:

    def print_context(**kwargs):
        print("This is the context task")
        print("The execution date is:", kwargs['logical_date'])
        print("The dag run id is:", kwargs['dag_run'].run_id)

    context_task = PythonOperator(
        task_id="context_task",
        python_callable=print_context
    )

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo 'This is the bash task'"
    )

    context_task >> bash_task