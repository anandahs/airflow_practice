"""
SQS Event-Driven DAG
====================
Triggers automatically when a message lands in the SQS queue.

Event chain:
  S3 upload → SNS → SQS → AssetWatcher → This DAG triggers

Setup:
  - SQS_QUEUE_URL must be set in .env
  - aws_default connection must be configured in Airflow UI
"""

import os
from airflow.sdk import dag, task, Asset
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk.definitions.asset import AssetWatcher

SQS_QUEUE_URL = os.environ.get(
    "SQS_QUEUE_URL",
    "https://sqs.us-east-1.amazonaws.com/071195298597/airflow-events"
)

# ── Asset watched by SQS ──────────────────────────────────────────────────────
# AssetWatcher polls SQS via the Triggerer component (no worker slot blocked)
# When a message arrives → asset is updated → DAG triggers
sqs_asset = Asset(
    name="sqs_queue_asset",
    watchers=[
        AssetWatcher(
            name="sqs_watcher",
            trigger=MessageQueueTrigger(
                queue=SQS_QUEUE_URL,
                aws_conn_id="aws_default",
            ),
        )
    ],
)


@dag(
    schedule=[sqs_asset],      # triggers when sqs_asset is updated
    catchup=False,
    tags=["event-driven", "sqs", "aws"],
)
def sqs_triggered_pipeline():
    """
    Pipeline triggered by SQS messages.
    Reads the message payload and processes it.
    """

    @task
    def read_message(**context) -> dict:
        """Read the SQS message payload from the asset event."""
        asset_events = context.get("triggering_asset_events", {})
        events = asset_events.get("sqs_queue_asset", [])

        if events:
            message = events[0].extra
            print(f"📨 Received SQS message: {message}")
            return message if isinstance(message, dict) else {"raw": str(message)}

        print("⚠️  No message payload found in asset event")
        return {}

    @task
    def process_message(message: dict) -> dict:
        """
        Process the message payload.
        In real use: trigger ETL, call API, load to warehouse, etc.
        """
        print(f"⚙️  Processing message: {message}")

        # Check if this came from S3 via SNS
        if "Records" in message:
            for record in message["Records"]:
                if record.get("eventSource") == "aws:s3":
                    bucket = record["s3"]["bucket"]["name"]
                    key    = record["s3"]["object"]["key"]
                    size   = record["s3"]["object"]["size"]
                    print(f"📁 S3 file detected!")
                    print(f"   Bucket : {bucket}")
                    print(f"   Key    : {key}")
                    print(f"   Size   : {size} bytes")
                    return {"source": "s3", "bucket": bucket, "key": key, "size": size}

        # Direct SQS message (not from S3)
        return {"source": "direct_sqs", "payload": message}

    @task
    def notify_completion(result: dict) -> None:
        """Log completion — in production: send Slack/email alert."""
        source = result.get("source", "unknown")

        if source == "s3":
            print(f"✅ Processed S3 file: s3://{result['bucket']}/{result['key']}")
        elif source == "direct_sqs":
            print(f"✅ Processed direct SQS message: {result['payload']}")
        else:
            print(f"✅ Pipeline complete: {result}")

    # Wire tasks
    message = read_message()
    result  = process_message(message)
    notify_completion(result)


sqs_triggered_pipeline()
