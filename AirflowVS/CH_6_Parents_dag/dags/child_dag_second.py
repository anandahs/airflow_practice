from airflow.sdk import dag, task
import os

@dag(dag_id="child_dag_second", schedule=None, start_date=None, catchup=False, is_paused_upon_creation=False)
def child_dag_second():

    @task.python
    def task_pre():
        print("This is the child DAG task")

    @task.python
    def task_write():
        os.makedirs("/tmp/data", exist_ok=True)
        with open("/tmp/data/output_second.txt", "w") as f:
            f.write("This is the second child DAG")

    task_pre() >> task_write()

child_dag_second_dag = child_dag_second()