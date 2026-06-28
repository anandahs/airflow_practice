import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from utils.bronze_layer import BronzeLayer
from utils.silver_layer import SilverLayer
from airflow.sdk import dag, task
from datetime import datetime, timedelta

@dag(schedule='@daily', is_paused_upon_creation=False, catchup=False)
def aws_pipeline():
   
   @task.python(retries=3, retry_delay=timedelta(seconds=5))
   def extract_load_to_s3():
      # urls for files to be downloaded from git
    
      urls = ["https://raw.githubusercontent.com/anandahs/airflow_practice/refs/heads/main/airflow_ingest/airports.csv",
              "https://raw.githubusercontent.com/anandahs/airflow_practice/refs/heads/main/airflow_ingest/bookings.csv",
              "https://raw.githubusercontent.com/anandahs/airflow_practice/refs/heads/main/airflow_ingest/passengers.csv"]
      
      bronzelayer = BronzeLayer()

      folder_name = datetime.now().strftime("%Y-%m-%d")

      for url in urls:
         print(f"downloading file from:{url}")
         fetched_data = bronzelayer.ingest_data_api(url=url)
         bronzelayer.put_data_s3("airflow-aws-ananda", f"bronze/{folder_name}/{url.split("/")[-1]}", fetched_data)
   
      return folder_name
   
   @task.python(retries=3, retry_delay=timedelta(seconds=5))
   def transform_load_s3(ti):
      last_load_date = ti.xcom_pull(task_ids="extract_load_to_s3", key="return_value")

      obj = SilverLayer()

      job_run_id = obj.trigger_spark_job("silver_layer", last_load_date)
      print(f"Glue job triggered with JobRunId:{job_run_id} to write delta")

   @task.python(retries=3, retry_delay=timedelta(seconds=5))
   def transform_load_s3_parquet(ti):
      last_load_date = ti.xcom_pull(task_ids="extract_load_to_s3", key="return_value")

      obj = SilverLayer()

      job_run_id = obj.trigger_spark_job("silver_layer_athena", last_load_date)
      print(f"Glue job triggered with JobRunId:{job_run_id} to write parquet")

    # task to trigger glue crawler
   @task.python(retries=3, retry_delay=timedelta(seconds=5))
   def trigger_glue_crawler():
      obj = SilverLayer()
      craweler_id= obj.trigger_glue_crawler("airflow_s3_crawler_silver")
      print(f"Glue crawler is triggered with crawler_id:{craweler_id} to read parquet data")


   extract_load = extract_load_to_s3()
   transfer_load = transform_load_s3()
   transform_load_s3_parquet = transform_load_s3_parquet()
   trigger_glue_crawler = trigger_glue_crawler()

   extract_load >> [transfer_load, transform_load_s3_parquet] >> trigger_glue_crawler

aws_pipeline()


         

      
