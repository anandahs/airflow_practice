from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
import pendulum


@dag(
     dag_id="schedule_basic",
     start_date=pendulum.datetime(year=2026, month=1, day=1, tz="America/New_York"),
     schedule="30 21 * * *", 
     catchup=False,
     is_paused_upon_creation=False)
def schedule_basic():
    @task.python
    def fetch_data(do_xcom_push=True) -> dict: # this is true by default, but you can set it to False to not push to XCom
        data = {"name": "Airflow", "version": "3.0"}
        return data
    
    @task.python
    def process_data(pulled_data: dict): # this will automatically pull the return value of fetch_data from XCom
        processed_data = f"Processed {pulled_data['name']} version {pulled_data['version']}"
        print(processed_data)

    bash_operation = BashOperator(
        task_id="bash_task",
        bash_command="echo 'This is a bash task'"
    )

    pulled_data = fetch_data()
    process_data(pulled_data) >> bash_operation

schedule_basic_instance = schedule_basic()