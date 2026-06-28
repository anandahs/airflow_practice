import boto3
import os
import time
from dotenv import load_dotenv

class SilverLayer:

    SUCCESS_STATES = ["SUCCEEDED"]
    FAILURE_STATES = ["FAILED", "STOPPED", "TIMEOUT", "ERROR"]

    def __init__(self):
        pass

    def trigger_spark_job(self, job_name, job_parameter, poll_interval=30, timeout=3600):

        aws_access_key_id= os.getenv("aws_access_key_id")
        aws_secret_access_key = os.getenv("aws_secret_access_key")
        aws_region = 'us-east-1'

        # create a glue client

        glue_client = boto3.client("glue",
                                   region_name = aws_region,
                                   aws_access_key_id = aws_access_key_id,
                                   aws_secret_access_key = aws_secret_access_key)

        response = glue_client.start_job_run(
            JobName = job_name,
            Arguments = {
                "--load_date" : job_parameter
            }
        )

        job_run_id = response['JobRunId']

        start_time = time.time()
        while True:
            run = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
            state = run['JobRun']['JobRunState']

            if state in self.SUCCESS_STATES:
                return job_run_id

            if state in self.FAILURE_STATES:
                error_message = run['JobRun'].get('ErrorMessage', 'No error message provided')
                raise RuntimeError(f"Glue job {job_name} (run {job_run_id}) ended in state {state}: {error_message}")

            if time.time() - start_time > timeout:
                raise TimeoutError(f"Glue job {job_name} (run {job_run_id}) did not finish within {timeout} seconds")

            time.sleep(poll_interval)

    def trigger_glue_crawler(self, crawler_name, poll_interval=30, timeout=3600):
        aws_access_key_id= os.getenv("aws_access_key_id")
        aws_secret_access_key = os.getenv("aws_secret_access_key")
        aws_region = 'us-east-1'

        # create a glue client

        glue_client = boto3.client("glue",
                                   region_name = aws_region,
                                   aws_access_key_id = aws_access_key_id,
                                   aws_secret_access_key = aws_secret_access_key)

        glue_client.start_crawler(Name=crawler_name)

        start_time = time.time()
        while True:
            crawler = glue_client.get_crawler(Name=crawler_name)['Crawler']
            state = crawler['State']

            if state == "READY":
                last_crawl = crawler.get('LastCrawl', {})
                status = last_crawl.get('Status')

                if status in self.FAILURE_STATES:
                    error_message = last_crawl.get('ErrorMessage', 'No error message provided')
                    raise RuntimeError(f"Glue crawler {crawler_name} ended in status {status}: {error_message}")

                return status

            if time.time() - start_time > timeout:
                raise TimeoutError(f"Glue crawler {crawler_name} did not finish within {timeout} seconds")

            time.sleep(poll_interval)

if __name__ == "__main__":
    obj = SilverLayer()
    job_run_id = obj.trigger_spark_job("silver_layer", "2026-06-27")
    print(f"Glue job finished with job_run_id:{job_run_id}")

