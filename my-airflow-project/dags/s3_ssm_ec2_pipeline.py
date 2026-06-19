"""
S3 → SSM → EC2 Pipeline
========================
Simulates MWAA → SSM → EC2 pattern used in organizations.

Flow:
  1. S3KeySensor waits for CSV file in incoming/ (every 5 mins, deferrable)
  2. Get ONE file at a time from S3 (sorted — oldest first)
  3. Discover EC2 instances via tags (simulates Cloud Map)
  4. Dynamically map SSM shell script to ALL instances in parallel
  5. Wait for all SSM commands to complete
  6. Archive processed file to processed/ folder
  7. Notify completion
  8. DAG restarts automatically (@continuous) — picks next file

Changes from original:
  - schedule="@continuous"  → restarts after each run automatically
  - max_active_runs=1       → processes ONE file at a time, no overlap
  - get_file_details        → picks oldest file only (not all files)
  - archive_s3_file         → archives ONE file only (not all files)

Environment variables required in .env:
  S3_BUCKET          = airflow-demo-shiva
  EC2_INSTANCE_IDS   = i-xxxxx,i-yyyyy  (comma separated, or single)
  EC2_REGION         = us-east-1
  EC2_SCRIPT_PATH    = /home/ec2-user/app/process_file.sh
"""

import os
import time
from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# ── Config ────────────────────────────────────────────────────────────────────
S3_BUCKET        = os.environ.get("S3_BUCKET", "airflow-demo-shiva")
S3_PREFIX        = "incoming/"
S3_KEY_PATTERN   = "incoming/*.csv"
EC2_REGION       = os.environ.get("EC2_REGION", "us-east-1")
EC2_SCRIPT_PATH  = os.environ.get("EC2_SCRIPT_PATH",
                                   "/home/ec2-user/app/process_file.sh")
EC2_INSTANCE_IDS = os.environ.get("EC2_INSTANCE_IDS", "")


@dag(
    schedule="@continuous",         # ← KEY CHANGE: restarts after each run
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,              # ← KEY CHANGE: one file at a time
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["s3", "ssm", "ec2", "continuous"],
)
def s3_ssm_ec2_pipeline():
    """
    Replaces Autosys S3 polling + EC2 trigger pattern with Airflow.
    Uses S3KeySensor (deferrable) + SSM (no SSH keys needed).
    Processes ONE file per run. Restarts automatically after each file.
    Simulates how MWAA → SSM → EC2 works in organizations.
    """

    # ── Step 1: Wait for S3 file ──────────────────────────────────────────────
    # mode="reschedule" = Triggerer handles wait = 0 worker slots blocked
    # After each completed run, DAG restarts and sensor waits again
    wait_for_file = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_name=S3_BUCKET,
        bucket_key=S3_KEY_PATTERN,
        wildcard_match=True,
        aws_conn_id="aws_default",
        mode="reschedule",          # deferrable — Triggerer handles this
        poke_interval=60 * 5,       # check every 5 mins (same as Autosys)
        timeout=60 * 60 * 24,       # give up after 24 hours
        soft_fail=False,
    )

    # ── Step 2: Get ONE file at a time ────────────────────────────────────────
    @task
    def get_file_details(**context) -> dict:
        """
        Find ONE CSV file in S3 incoming/ folder.
        Always picks the oldest file first (sorted alphabetically).
        Remaining files are processed in next DAG run (@continuous).
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="aws_default")
        keys = hook.list_keys(bucket_name=S3_BUCKET, prefix=S3_PREFIX)

        # Filter to CSV files and sort — oldest first
        csv_files = sorted([
            k for k in (keys or []) 
            if k.endswith(".csv")
        ])

        if not csv_files:
            raise ValueError(
                f"No CSV files found in s3://{S3_BUCKET}/{S3_PREFIX}"
            )

        # Process ONE file only — remaining picked up in next run
        primary_file = csv_files[0]

        print(f"📁 Total files in incoming/ : {len(csv_files)}")
        print(f"📁 Processing now           : s3://{S3_BUCKET}/{primary_file}")

        if len(csv_files) > 1:
            print(f"📁 Queued for next runs     : {len(csv_files) - 1} file(s)")
            for f in csv_files[1:]:
                print(f"   ⏳ {f}")

        return {
            "bucket":       S3_BUCKET,
            "primary_file": primary_file,   # ONE file only
            "triggered_at": context["dag_run"].run_after.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        }

    # ── Step 3: Discover EC2 instances ───────────────────────────────────────
    @task
    def discover_ec2_instances() -> list[dict]:
        """
        Discover EC2 instances to run the processing script on.

        Option 1: From .env EC2_INSTANCE_IDS (comma-separated) — simplest
        Option 2: Tag-based discovery — simulates Cloud Map service discovery

        In your org with Cloud Map:
        Replace with boto3 servicediscovery.discover_instances()
        """
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        ec2 = AwsBaseHook(
            aws_conn_id="aws_default",
            client_type="ec2"
        ).get_client_type(region_name=EC2_REGION)

        # Option 1 — Instance IDs from .env
        if EC2_INSTANCE_IDS:
            instance_ids = [
                i.strip() for i in EC2_INSTANCE_IDS.split(",")
                if i.strip()
            ]
            print(f"📋 Using {len(instance_ids)} instance(s) from .env:")
            for iid in instance_ids:
                print(f"   {iid}")
            return [{"instance_id": iid} for iid in instance_ids]

        # Option 2 — Tag-based discovery (simulates Cloud Map)
        print("🔍 Discovering EC2 instances by tag...")
        response = ec2.describe_instances(
            Filters=[
                {"Name": "tag:ManagedBy",       "Values": ["airflow-ssm"]},
                {"Name": "instance-state-name", "Values": ["running"]},
            ]
        )

        instances = []
        for reservation in response["Reservations"]:
            for instance in reservation["Instances"]:
                instance_id = instance["InstanceId"]
                name = next(
                    (t["Value"] for t in instance.get("Tags", [])
                     if t["Key"] == "Name"),
                    "unknown"
                )
                instances.append({
                    "instance_id": instance_id,
                    "name":        name,
                })
                print(f"   Found: {instance_id} ({name})")

        if not instances:
            raise ValueError(
                "No running EC2 instances found with tag ManagedBy=airflow-ssm"
            )

        print(f"✅ Discovered {len(instances)} instance(s)")
        return instances

    # ── Step 4: Run shell script on EC2 via SSM ───────────────────────────────
    @task
    def run_ssm_command(
        instance: dict,
        file_details: dict,
    ) -> dict:
        """
        Send SSM command to ONE EC2 instance.
        Dynamically mapped — runs in parallel on all discovered instances.
        No SSH keys, no open ports — pure SSM!
        """
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        ssm = AwsBaseHook(
            aws_conn_id="aws_default",
            client_type="ssm"
        ).get_client_type(region_name=EC2_REGION)

        instance_id = instance["instance_id"]
        bucket      = file_details["bucket"]
        file_key    = file_details["primary_file"]

        print(f"📤 Sending SSM command to {instance_id}...")
        print(f"   Script : {EC2_SCRIPT_PATH}")
        print(f"   Bucket : {bucket}")
        print(f"   File   : {file_key}")

        response = ssm.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={
                "commands": [
                    f"bash {EC2_SCRIPT_PATH} --bucket {bucket} --file {file_key}"
                ],
                "executionTimeout": ["3600"],
            },
            Comment=f"Airflow triggered: {file_key}",
        )

        command_id = response["Command"]["CommandId"]
        print(f"✅ SSM Command sent! Command ID: {command_id}")

        # ── Wait for command to complete ──────────────────────────────────────
        print(f"⏳ Waiting for command on {instance_id}...")
        max_wait    = 3600
        check_every = 15
        elapsed     = 0

        while elapsed < max_wait:
            time.sleep(check_every)
            elapsed += check_every

            invocation = ssm.get_command_invocation(
                CommandId=command_id,
                InstanceId=instance_id,
            )
            status = invocation["Status"]
            print(f"   [{elapsed}s] Status: {status}")

            if status == "Success":
                stdout = invocation.get("StandardOutputContent", "")
                print(f"✅ Succeeded on {instance_id}!")
                print(f"   Output:\n{stdout}")
                return {
                    "instance_id": instance_id,
                    "command_id":  command_id,
                    "status":      "success",
                    "output":      stdout,
                    "file":        file_key,
                }

            elif status in ["Failed", "Cancelled", "TimedOut", "DeliveryTimedOut"]:
                stderr = invocation.get("StandardErrorContent", "")
                raise Exception(
                    f"SSM failed on {instance_id}!\n"
                    f"Status : {status}\n"
                    f"Error  : {stderr}"
                )

        raise TimeoutError(
            f"SSM command on {instance_id} did not complete within {max_wait}s"
        )

    # ── Step 5: Archive S3 file ───────────────────────────────────────────────
    @task
    def archive_s3_file(
        file_details: dict,
        ssm_results: list,
    ) -> dict:
        """
        Move ONE processed file from incoming/ to processed/.
        Only archives AFTER all SSM commands succeed.
        @continuous restarts DAG → next file picked up automatically.
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="aws_default")

        # Ensure all instances succeeded before archiving
        failed = [r for r in ssm_results if r.get("status") != "success"]
        if failed:
            raise Exception(
                f"Cannot archive — {len(failed)} instance(s) failed!"
            )

        # Archive ONE file only
        file_key    = file_details["primary_file"]
        archive_key = file_key.replace("incoming/", "processed/", 1)

        print(f"📦 Archiving: {file_key} → {archive_key}")

        hook.copy_object(
            source_bucket_key=file_key,
            dest_bucket_key=archive_key,
            source_bucket_name=S3_BUCKET,
            dest_bucket_name=S3_BUCKET,
        )
        hook.delete_objects(
            bucket=S3_BUCKET,
            keys=[file_key],
        )

        print(f"✅ Archived: s3://{S3_BUCKET}/{archive_key}")
        print(f"   DAG will restart automatically for next file")

        return {
            "archived_file": archive_key,
            "count":         1,
        }

    # ── Step 6: Notify completion ─────────────────────────────────────────────
    @task
    def notify_completion(
        archive_result: dict,
        ssm_results: list,
    ) -> None:
        """
        Log pipeline completion summary.
        In production: send Slack/email alert here.
        DAG restarts automatically after this task (@continuous).
        """
        print("=" * 50)
        print("  ✅ File Processed Successfully!")
        print("=" * 50)
        print(f"  Archived       : {archive_result['archived_file']}")
        print(f"  EC2 instances  : {len(ssm_results)}")
        print("")
        print("  Instance results:")
        for result in ssm_results:
            print(f"   ✅ {result['instance_id']} — {result['status']}")
        print("")
        print("  ♻️  DAG restarting — watching for next file...")
        print("=" * 50)

    # ── Wire tasks together ───────────────────────────────────────────────────
    file_details = get_file_details()
    instances    = discover_ec2_instances()

    ssm_results = run_ssm_command.partial(
        file_details=file_details,
    ).expand(
        instance=instances,
    )

    archive_result = archive_s3_file(
        file_details=file_details,
        ssm_results=ssm_results,
    )

    notify_completion(
        archive_result=archive_result,
        ssm_results=ssm_results,
    )

    wait_for_file >> file_details


s3_ssm_ec2_pipeline()