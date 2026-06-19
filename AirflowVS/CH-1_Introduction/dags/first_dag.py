from airflow.sdk import dag, task


@dag(dag_id="first_dag", schedule=None, start_date=None, catchup=False)
def first_dag():
    @task(task_id="task_1")
    def task_1():
        print("This is task 1")
    
    @task(task_id="task_2")
    def task_2():
        print("This is task 2")

    @task(task_id="task_3")
    def task_3():
        print("This is task 3")

    # define the task dependencies

    t1 = task_1()
    t2 = task_2()
    t3 = task_3()

    t1 >> t2 >> t3

# to register the DAG, we need to call the function

first_dag_instance = first_dag()

    