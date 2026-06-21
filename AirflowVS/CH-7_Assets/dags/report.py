from airflow.sdk import dag, task
from airflow.sdk.definitions.asset import Asset
import json
from etl import weather_asset

@dag(dag_id="report_dag", schedule=[weather_asset], start_date=None, catchup=False)
def report_dag():

    @task.python
    def read_data():
        with open("/tmp/airflow/data/data.json", "r") as f:
            return json.load(f)
        
    read_data()
        
report_dag = report_dag()
        


