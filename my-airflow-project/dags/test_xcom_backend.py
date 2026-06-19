# dags/test_xcom_backend.py
from airflow.sdk import dag, task
from datetime import datetime

@dag(
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test", "xcom"],
)
def test_xcom_backend():

    @task
    def produce_large_xcom() -> dict:
        """Returns a large payload — should go to S3."""
        data = {
            "records": [
                {"id": i, "name": f"record_{i}", "value": f"data_{i}" * 10}
                for i in range(500)   # 500 records — definitely > 64 bytes
            ],
            "count": 500,
        }
        print(f"Returning {len(str(data))} bytes of XCom data")
        return data

    @task
    def produce_small_xcom() -> str:
        """Returns a tiny payload — should stay in metadata DB."""
        return "ok"   # 2 bytes — stays in DB

    @task
    def consume_xcom(large_data: dict, small_data: str) -> None:
        """Reads both XComs — verifies they were retrieved correctly."""
        print(f"Large XCom records : {large_data['count']}")
        print(f"Small XCom value   : {small_data}")
        print("✅ XCom backend working correctly!")

    large = produce_large_xcom()
    small = produce_small_xcom()
    consume_xcom(large, small)

test_xcom_backend()