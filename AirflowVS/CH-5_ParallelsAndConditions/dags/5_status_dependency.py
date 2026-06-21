from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule


@dag(dag_id="status_dependency_dag", schedule=None, start_date=None, catchup=False)
def status_dependency_dag():

    @task.python
    def task_a():
        print("This is task A")
        return "Task A completed"

    @task.python
    def task_b():
        print("This is task B")
        raise ValueError("Task B failed")
        #return "Task B completed"

    @task.python
    def task_c():
        print("This is task C")
        return "Task C completed"
    
    @task.python(trigger_rule=TriggerRule.ALL_DONE)
    def task_d():
        print("This is task D")
        return "Task D completed"

    task_a = task_a()
    task_b = task_b()
    task_c = task_c()
    task_d = task_d()

    task_a >> [task_b, task_c] >> task_d

status_dependency_dag_instance = status_dependency_dag()