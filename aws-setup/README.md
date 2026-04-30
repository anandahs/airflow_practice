# AWS Setup Scripts

## Order to run:
1. setup_aws_airflow.sh            - Creates SQS, S3, SNS resources
2. setup_airflow_aws_connection.sh - Configures aws_default in Airflow UI
3. cleanup_aws_airflow.sh          - Destroys all resources when done

## Requirements:
- AWS CLI configured (~/.aws/credentials)
- Astro CLI running (astro dev start)

## Verify connection:
```
astro dev run connections get aws_default
```

# unpause dags on UI
astro dev run dags unpause s3_file_arrival_pipeline
astro dev run dags unpause sqs_triggered_pipeline

# Test 1 — SQS direct message:
aws sqs send-message \
  --queue-url https://sqs.us-east-1.amazonaws.com/071195298597/airflow-events \
  --message-body '{"event": "test", "file": "data.csv", "source": "manual"}' \
  --region us-east-1

# Test 2 — S3 → SNS → SQS chain (triggers sqs_triggered_pipeline):
echo "id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300" > /tmp/test.csv

aws s3 cp /tmp/test.csv s3://airflow-demo-shiva/events/test.csv

# Test 3 — S3 only (triggers s3_file_arrival_pipeline — manually unpause + trigger DAG first):
aws s3 cp /tmp/test.csv s3://airflow-demo-shiva/incoming/test.csv

  

