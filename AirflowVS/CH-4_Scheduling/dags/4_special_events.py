from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
import pendulum
from airflow.timetables.events import EventsTimetable

events_list_obj =  EventsTimetable(event_dates=[
    pendulum.datetime(year=2026, month=6, day=20, tz="America/New_York"),
    pendulum.datetime(year=2026, month=6, day=21, tz="America/New_York"),
    pendulum.datetime(year=2026, month=6, day=27, tz="America/New_York"),
])

@dag(
     dag_id="special_events_dag",
     schedule=events_list_obj, # this will set the start date to 10 days from the time the DAG is unpaused
     start_date=pendulum.datetime(year=2026, month=6, day=20, tz="America/New_York"),
     is_paused_upon_creation=True)
def special_events_dag():
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

special_events_dag_instance = special_events_dag()