from airflow.sdk import dag, task
import os
import json
from airflow.sdk.definitions.asset import Asset

weather_asset = Asset(uri="fle:///opt/airflow/data/data.json")

@dag(dag_id="etl_dag", schedule=None, start_date=None, catchup=False, is_paused_upon_creation=False)
def etl_dag():
    
    @task.python
    def extract(ti):
        return {"data": {"city": "New York", "temperature": 30}}

    @task.python
    def transform(ti):
        data = ti.xcom_pull(task_ids="extract", key="return_value")
        city = data['data']['city']
        temperature_celsius = data['data']['temperature']
        temperature_fahrenheit = (temperature_celsius * 9/5) + 32
        transformed_data = {
            "city": city,
            "temperature_celsius": temperature_celsius,
            "temperature_fahrenheit": temperature_fahrenheit
        }
        return transformed_data

    @task.python(outlets=[weather_asset])
    def load(ti):
        transformed_data = ti.xcom_pull(task_ids="transform", key="return_value")
        os.makedirs("/tmp/airflow/data", exist_ok=True)
        with open("/tmp/airflow/data/data.json", "w") as f:
            json.dump(transformed_data, f)

    extract() >> transform() >> load()

etl_dag_instance = etl_dag()
