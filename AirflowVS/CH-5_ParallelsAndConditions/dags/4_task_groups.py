from airflow.sdk import dag, task, task_group
import pendulum

@dag(dag_id="task_groups_dag", schedule=None, start_date=pendulum.datetime(year=2026, month=6, day=15), catchup=False, is_paused_upon_creation=False)
def task_groups_dag():

    @task.bash
    def task_bash():
        return """
        echo "hello from bash task"
        """
    
    @task_group
    def data_fetching_group():  # this is just a logical grouping, it does not affect the execution of tasks
        @task.python
        def fetch_api():
            data = {"type": "api", "data": ["data1", "data2", "data3"]}
            return data

        @task.python
        def fetch_db():
            data = {"type": "db", "data": ["data4", "data5", "data6"]}
            return data

        @task.python
        def fetch_s3():
            data = {"type": "s3", "data": ["data7", "data8", "data9"]}
            return data
        
        [fetch_api(), fetch_db(), fetch_s3()]  # this will run the three tasks in parallel

    @task.python
    def process_data(ti):
        api_data = ti.xcom_pull(task_ids="data_fetching_group.fetch_api", key="return_value")
        db_data = ti.xcom_pull(task_ids="data_fetching_group.fetch_db", key="return_value")
        s3_data = ti.xcom_pull(task_ids="data_fetching_group.fetch_s3", key="return_value")

        print(f"Processing {api_data['type']} with data {api_data['data']}")
        print(f"Processing {db_data['type']} with data {db_data['data']}")
        print(f"Processing {s3_data['type']} with data {s3_data['data']}")

        return api_data['data'] + db_data['data'] + s3_data['data']
    
    @task.branch
    def decide_branch(ti):
        data = ti.xcom_pull(task_ids="process_data", key="return_value")
        if len(data) > 10:
            return "s3_load_task"
        else:
            return "glue_task"
        
    @task.python
    def s3_load_task():
        print("Loading data to S3")

    @task.python
    def glue_task():
        print("Running Glue job")

    task_bash = task_bash() # this will run independently of the other tasks

    task_bash >> data_fetching_group() >> process_data() >> decide_branch() >> [s3_load_task(), glue_task()]

task_groups_dag_instance = task_groups_dag()