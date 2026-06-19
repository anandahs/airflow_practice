from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator

def first_task():
    print("Hello, World!")

@dag(dag_id="python_dag", schedule=None, start_date=None, catchup=False)
def python_dag():

    second_task = PythonOperator(
        task_id="second_task",
        python_callable=lambda: print("This is the second task")
    )

    first_task_op = PythonOperator(
        task_id="first_task",
        python_callable=first_task
    )

    first_task_op >> second_task    

python_dag_instance = python_dag()