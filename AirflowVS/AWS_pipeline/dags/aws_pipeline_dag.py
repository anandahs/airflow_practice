import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from utils.bronze_layer import BronzeLayer
from airflow.sdk import dag, task
from datetime import timedelta

@dag(schedule='@daily', is_paused_upon_creation=False, catchup=False)
def aws_pipeline():
   
   @task.python(retries=3, retry_delay=timedelta(seconds=5))
   def extract_load():
      # urls for files to be downloaded from git
    
      urls = ["https://raw.githubusercontent.com/anandahs/airflow_practice/refs/heads/main/airflow_ingest/airports.csv",
              "https://raw.githubusercontent.com/anandahs/airflow_practice/refs/heads/main/airflow_ingest/bookings.csv",
              "https://raw.githubusercontent.com/anandahs/airflow_practice/refs/heads/main/airflow_ingest/passengers.csv"]
      
      bronzelayer = BronzeLayer()

      for url in urls:
         fetched_data = bronzelayer.ingest_data_api(url=url)
         bronzelayer.put_data_s3("airflow-aws-ananda", f"bronze/{url.split("/")[-1]}", fetched_data)
   
   extract_load = extract_load()

aws_pipeline()


         

      
