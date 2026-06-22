from airflow.sdk import dag, task

@dag(dag_id='compute_heavy_dag', start_date=None, schedule=None, is_paused_upon_creation=True, catchup=False)
def compute_heavy_dag():

    @task.python
    def task_a():
        print("task a")

    @task
    def task_b():
        print("task b")

    @task
    def task_c():
        print("task c")

    @task(queue='compute_heavy')
    def task_d():
        print("task d")

    t1 = task_a()
    t2 = task_b()
    t3 = task_c()
    t4 = task_d()

    [t1, t2, t3] >> t4

compute_heavy_dag = compute_heavy_dag()

    