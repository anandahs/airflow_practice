from airflow.sdk import dag, task
import pendulum

@dag(dag_id="parallel_dag", schedule=None, start_date=pendulum.datetime(year=2026, month=6, day=15), catchup=False, is_paused_upon_creation=False)
def parallel_dag():

    @task.bash
    def task_bash():
        return """
        echo "hello from bash task"
        """
    
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

    @task.python
    def process_data(api_data, db_data, s3_data):
        print(f"Processing {api_data['type']} with data {api_data['data']}")
        print(f"Processing {db_data['type']} with data {db_data['data']}")
        print(f"Processing {s3_data['type']} with data {s3_data['data']}")

    task_bash = task_bash() # this will run independently of the other tasks
    api_data = fetch_api()
    db_data = fetch_db()
    s3_data = fetch_s3()

    task_bash >> [api_data, db_data, s3_data ] >> process_data(api_data, db_data, s3_data)

parallel_dag_instance = parallel_dag()