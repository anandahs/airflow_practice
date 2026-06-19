
from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator

@dag(dag_id="logical_date_dag", schedule=None, start_date=None, catchup=False)
def logical_date_dag():
    
    @task.python
    def print_logical_date(**context): # kwargs can be used as context as well, but context is more descriptives
        logical_date = context['logical_date']
        print(f"The logical date for this task is: {logical_date}")

    @task.bash
    def bash_logical_date():
        return "echo 'This is a bash task with logical date: {{logical_date | ds}}'"
    
    print_logical_date() >> bash_logical_date()

logical_date_dag_instance = logical_date_dag()