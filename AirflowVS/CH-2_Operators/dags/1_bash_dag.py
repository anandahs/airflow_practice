from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

@dag(dag_id="bash_dag", schedule=None, start_date=None, catchup=False)
def bash_dag():

    @task.bash
    def print_hello():
        return "echo 'Hello, World!'"
    
    second_task = BashOperator(
        task_id="second_task",
        bash_command="echo 'This is the second task'"
    )

    first_task = print_hello()
    first_task >> second_task

bash_dag_instance = bash_dag()


