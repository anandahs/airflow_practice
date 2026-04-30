"""
S3 Event-Driven DAG
===================
Triggers automatically when a CSV file lands in S3 incoming/ folder.
Uses S3KeySensor (reschedule mode) with a schedule so runs are always active.

Event chain:
  File uploaded to s3://airflow-demo-shiva/incoming/
    → S3KeySensor detects it (polls every 30s, releases worker slot between checks)
    → Downstream tasks process and archive the file
    → Next scheduled run starts and waits for the next file
"""

import os
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_BUCKET = os.environ.get("S3_BUCKET", "airflow-demo-shiva")


@dag(
    schedule="*/5 * * * *",   # new run every 5 min — sensor fires immediately if file exists
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,         # only one run waits at a time
    tags=["event-driven", "s3", "aws"],
)
def s3_file_arrival_pipeline():
    """
    Pipeline triggered when a CSV file lands in S3 incoming/ folder.
    Reads metadata → processes → archives the file.
    """

    wait_for_file = S3KeySensor(
        task_id="wait_for_file",
        bucket_name=S3_BUCKET,
        bucket_key="incoming/*.csv",
        wildcard_match=True,
        aws_conn_id="aws_default",
        mode="reschedule",      # releases worker slot between pokes
        timeout=60 * 5,         # give up after 5 min (next run takes over)
        poke_interval=30,
        soft_fail=True,         # skip cleanly if no file found within timeout
    )

    @task
    def get_file_info(**context) -> dict:
        """Find which file(s) landed in S3 incoming/ folder."""
        hook      = S3Hook(aws_conn_id="aws_default")
        keys      = hook.list_keys(bucket_name=S3_BUCKET, prefix="incoming/")
        csv_files = [k for k in (keys or []) if k.endswith(".csv")]

        print(f"📁 Found {len(csv_files)} CSV file(s) in incoming/:")
        for f in csv_files:
            print(f"   - {f}")

        return {
            "bucket": S3_BUCKET,
            "files": csv_files,
            "count": len(csv_files),
            "triggered_at": context["dag_run"].run_after.isoformat(),
        }

    @task
    def process_file(file_info: dict) -> dict:
        """
        Process each file found in S3.
        In real use: read CSV → transform → load to Redshift/Snowflake.
        """
        results = []
        hook = S3Hook(aws_conn_id="aws_default")

        for key in file_info["files"]:
            print(f"⚙️  Processing: s3://{file_info['bucket']}/{key}")

            content = hook.read_key(key=key, bucket_name=file_info["bucket"])
            lines   = content.strip().split("\n")
            rows    = len(lines) - 1  # subtract header

            print(f"   Rows found : {rows}")
            print(f"   Header     : {lines[0] if lines else 'empty'}")

            results.append({"key": key, "rows": rows, "status": "processed"})

        print(f"✅ Processed {len(results)} file(s)")
        return {
            "bucket":     file_info["bucket"],
            "results":    results,
            "total_rows": sum(r["rows"] for r in results),
        }

    @task
    def archive_file(result: dict) -> None:
        """Move processed files from incoming/ to archive/."""
        hook = S3Hook(aws_conn_id="aws_default")

        for file_result in result["results"]:
            source_key  = file_result["key"]
            archive_key = source_key.replace("incoming/", "archive/", 1)

            print(f"📦 Archiving: {source_key} → {archive_key}")

            hook.copy_object(
                source_bucket_key=source_key,
                dest_bucket_key=archive_key,
                source_bucket_name=result["bucket"],
                dest_bucket_name=result["bucket"],
            )
            hook.delete_objects(bucket=result["bucket"], keys=[source_key])

            print(f"✅ Archived: {archive_key}")

        print(f"🎉 All done! {len(result['results'])} file(s) archived.")
        print(f"   Total rows processed: {result['total_rows']}")

    # ── Wire tasks ────────────────────────────────────────────────────────────
    file_info = get_file_info()
    result    = process_file(file_info)
    archive_file(result)

    wait_for_file >> file_info


s3_file_arrival_pipeline()
