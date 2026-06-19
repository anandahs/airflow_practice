from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

@dag(dag_id="xcom_auto", schedule=None, start_date=None, catchup=False)
def xcoms_auto():
 
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
    

xcoms_auto_instance = xcoms_auto()