from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta

@dag(dag_id="kwargs_dag", schedule=None, start_date=None, catchup=False)
def kwargs_dag():
 
    @task.python
    def fetch_data(**kwargs):
        print("Fetching kwargs...")
        print(f"Kwargs: {kwargs}")
        data = {"name": "Airflow", "version": "3.0"}
        #return data
        # pushing data manually to XCom
        ti = kwargs['ti']
        ti.xcom_push(key="fetched_data", value=data)
    
    @task.python
    def process_data(**kwargs):
        ti = kwargs['ti']
        pulled_data = ti.xcom_pull(task_ids="fetch_data", key="fetched_data")
        processed_data = f"Processed {pulled_data['name']} version {pulled_data['version']}"
        print(processed_data)

    bash_operation = BashOperator(
        task_id="bash_task",
        bash_command="echo 'This is a bash task with XCom data: {{ ti.xcom_pull(task_ids=\"fetch_data\", key=\"fetched_data\") }}'"
    )

    @task.bash(retries=3, retry_delay=timedelta(seconds=5))
    def bash_task_with_xcom(**kwargs):
        print("Executing bash task with XCom data...")

        #ti = kwargs['ti'] you don't need to pull XCom data here, you can directly use it in the bash_command template 
        # since it is run time variable
        my_var = 'Ananda Hariharashivamurthy'
        return f"""echo 'This is a {my_var} bash task with XCom data: {{{{ ti.xcom_pull(task_ids=\"fetch_data\", key=\"fetched_data\") }}}}'"""


    fetch = fetch_data()
    process = process_data()    
    fetch >> process >> bash_operation >> bash_task_with_xcom()

kwargs_dag_instance = kwargs_dag()