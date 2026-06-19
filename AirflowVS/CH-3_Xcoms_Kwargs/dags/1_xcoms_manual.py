from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

@dag(dag_id="xcom_manual", schedule=None, start_date=None, catchup=False)
def xcoms_manual():
 
    @task.python
    def fetch_data(ti):
        data = {"name": "Airflow", "version": "3.0"}
        #return data
        # pushing data manually to XCom
        ti.xcom_push(key="fetched_data", value=data)
        return data
    
    @task.python
    def process_data(ti):
        pulled_data = ti.xcom_pull(task_ids="fetch_data", key="fetched_data")
        processed_data = f"Processed {pulled_data['name']} version {pulled_data['version']}"
        print(processed_data)

    bash_operation = BashOperator(
        task_id="bash_task",
        bash_command="echo 'This is a bash task'"
    )

    fetch = fetch_data()
    process = process_data()    
    fetch >> process >> bash_operation

xcoms_manual_instance = xcoms_manual()