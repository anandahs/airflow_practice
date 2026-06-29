from databricks.sdk import WorkspaceClient
import os
from dotenv import load_dotenv

load_dotenv()

class GoldLayer:

    def __int__(self):
        pass

    def run_databricks_job(self, job_id:int):
        databricks_pat = os.getenv("DATABRICKS_PAT")
        databricks_host = os.getenv("DATABRICKS_HOST")

        client = WorkspaceClient(
            host = databricks_host,
            token = databricks_pat
        )

        run = client.jobs.run_now(job_id=job_id)

        print(f"triggered job id:{job_id}")

        result = run.result()

        print(f"job id:{job_id} finished with state:{result.state.result_state}")

        return result

if __name__ == "__main__":
    gold_layer = GoldLayer()
    gold_layer.run_databricks_job("1004107495038505")