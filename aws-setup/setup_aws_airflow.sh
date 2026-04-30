#!/bin/bash
# =============================================================================
# AWS Setup Script for Airflow Event-Driven DAGs
# Creates: SQS Queue, S3 Bucket, SNS Topic, SNS→SQS Subscription
#          S3→SNS Event Notification
# Usage: cd my-airflow-project && bash setup_aws_airflow.sh
# =============================================================================

set -e  # exit immediately on any error

# ── Config — change these if needed ──────────────────────────────────────────
REGION="us-east-1"
SQS_QUEUE_NAME="airflow-events"
S3_BUCKET_NAME="airflow-demo-shiva"
SNS_TOPIC_NAME="airflow-notifications"
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo "============================================="
echo "  Airflow AWS Event-Driven Setup"
echo "============================================="
echo ""

# ── Get AWS Account ID ────────────────────────────────────────────────────────
echo "🔍 Fetching AWS Account ID..."
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "✅ Account ID: $ACCOUNT_ID"
echo ""

# ── Step 1: Create SQS Queue ──────────────────────────────────────────────────
echo "📬 Step 1: Creating SQS queue: $SQS_QUEUE_NAME..."
SQS_URL=$(aws sqs create-queue \
  --queue-name "$SQS_QUEUE_NAME" \
  --region "$REGION" \
  --query QueueUrl \
  --output text)

SQS_ARN="arn:aws:sqs:$REGION:$ACCOUNT_ID:$SQS_QUEUE_NAME"
echo "✅ SQS URL : $SQS_URL"
echo "✅ SQS ARN : $SQS_ARN"
echo ""

# ── Step 2: Create S3 Bucket ──────────────────────────────────────────────────
echo "🪣  Step 2: Creating S3 bucket: $S3_BUCKET_NAME..."
aws s3 mb "s3://$S3_BUCKET_NAME" --region "$REGION" 2>/dev/null \
  || echo "⚠️  Bucket may already exist, continuing..."

# Create incoming/ prefix for s3_file_arrival_pipeline
echo "placeholder" | aws s3 cp - "s3://$S3_BUCKET_NAME/incoming/.keep" \
  --region "$REGION" > /dev/null

# Create events/ prefix for sqs_triggered_pipeline (S3→SNS→SQS chain)
echo "placeholder" | aws s3 cp - "s3://$S3_BUCKET_NAME/events/.keep" \
  --region "$REGION" > /dev/null

echo "✅ S3 Bucket : s3://$S3_BUCKET_NAME"
echo "✅ Created incoming/ prefix (for s3_file_arrival_pipeline)"
echo "✅ Created events/ prefix   (for sqs_triggered_pipeline)"
echo ""

# ── Step 3: Create SNS Topic ──────────────────────────────────────────────────
echo "📣 Step 3: Creating SNS topic: $SNS_TOPIC_NAME..."
SNS_ARN=$(aws sns create-topic \
  --name "$SNS_TOPIC_NAME" \
  --region "$REGION" \
  --query TopicArn \
  --output text)
echo "✅ SNS ARN : $SNS_ARN"
echo ""

# ── Step 4: Allow SNS to publish to SQS ──────────────────────────────────────
echo "🔐 Step 4: Setting SQS policy to accept SNS messages..."

SQS_POLICY=$(cat <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AllowSNSToSendToSQS",
    "Effect": "Allow",
    "Principal": {"Service": "sns.amazonaws.com"},
    "Action": "sqs:SendMessage",
    "Resource": "$SQS_ARN",
    "Condition": {
      "ArnEquals": {"aws:SourceArn": "$SNS_ARN"}
    }
  }]
}
POLICY
)

ATTR_FILE=$(mktemp /tmp/sqs_attrs_XXXXXX.json)
python3 -c "
import sys, json
policy = json.load(sys.stdin)
print(json.dumps({'Policy': json.dumps(policy)}))
" <<< "$SQS_POLICY" > "$ATTR_FILE"

aws sqs set-queue-attributes \
  --queue-url "$SQS_URL" \
  --attributes "file://$ATTR_FILE" \
  --region "$REGION"

rm -f "$ATTR_FILE"
echo "✅ SQS policy updated"
echo ""

# ── Step 5: Subscribe SQS to SNS ─────────────────────────────────────────────
echo "🔗 Step 5: Subscribing SQS to SNS topic..."
SUBSCRIPTION_ARN=$(aws sns subscribe \
  --topic-arn "$SNS_ARN" \
  --protocol sqs \
  --notification-endpoint "$SQS_ARN" \
  --region "$REGION" \
  --query SubscriptionArn \
  --output text)
echo "✅ Subscription ARN : $SUBSCRIPTION_ARN"
echo ""

# ── Step 6: Allow S3 to publish to SNS ───────────────────────────────────────
echo "🔔 Step 6: Allowing S3 to publish to SNS..."

SNS_POLICY=$(cat <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AllowS3ToPublish",
    "Effect": "Allow",
    "Principal": {"Service": "s3.amazonaws.com"},
    "Action": "sns:Publish",
    "Resource": "$SNS_ARN",
    "Condition": {
      "ArnLike": {
        "aws:SourceArn": "arn:aws:s3:::$S3_BUCKET_NAME"
      }
    }
  }]
}
POLICY
)

aws sns set-topic-attributes \
  --topic-arn "$SNS_ARN" \
  --attribute-name Policy \
  --attribute-value "$SNS_POLICY" \
  --region "$REGION"
echo "✅ SNS policy updated"
echo ""

# ── Step 7: Configure S3 → SNS Event Notification ────────────────────────────
echo "📡 Step 7: Configuring S3 to notify SNS on file uploads..."

S3_NOTIFICATION=$(cat <<NOTIFICATION
{
  "TopicConfigurations": [{
    "TopicArn": "$SNS_ARN",
    "Events": ["s3:ObjectCreated:*"],
    "Filter": {
      "Key": {
        "FilterRules": [{
          "Name": "prefix",
          "Value": "events/"
        }]
      }
    }
  }]
}
NOTIFICATION
)

aws s3api put-bucket-notification-configuration \
  --bucket "$S3_BUCKET_NAME" \
  --notification-configuration "$S3_NOTIFICATION"
echo "✅ S3 will notify SNS on uploads to events/"
echo ""

# ── Step 8: Write to .env ─────────────────────────────────────────────────────
echo "📝 Step 8: Writing config to .env..."
cat >> .env <<EOF

# ── AWS Event-Driven DAG Config (auto-generated) ──────
AWS_REGION=$REGION
SQS_QUEUE_URL=$SQS_URL
SQS_QUEUE_ARN=$SQS_ARN
SNS_TOPIC_ARN=$SNS_ARN
S3_BUCKET=$S3_BUCKET_NAME
AWS_ACCOUNT_ID=$ACCOUNT_ID
EOF
echo "✅ Values written to .env"
echo ""

# ── Step 9: Verify everything ─────────────────────────────────────────────────
echo "🔎 Step 9: Verifying setup..."
echo ""
echo "  SQS Queue:"
aws sqs get-queue-attributes \
  --queue-url "$SQS_URL" \
  --attribute-names QueueArn ApproximateNumberOfMessages \
  --region "$REGION" \
  --query 'Attributes' \
  --output table
echo ""
echo "  S3 Bucket:"
aws s3 ls "s3://$S3_BUCKET_NAME" --region "$REGION"
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "============================================="
echo "  ✅ All done! Setup Complete."
echo "============================================="
echo ""
echo "  Resources created:"
echo "  SQS  : $SQS_URL"
echo "  S3   : s3://$S3_BUCKET_NAME"
echo "  SNS  : $SNS_ARN"
echo ""
echo "  Prefix routing:"
echo "  incoming/  → S3KeySensor → s3_file_arrival_pipeline (trigger manually)"
echo "  events/    → SNS → SQS  → sqs_triggered_pipeline   (auto-triggered)"
echo ""
echo "  Test commands:"
echo ""
echo "  # Test SQS directly (sqs_triggered_pipeline):"
echo "  aws sqs send-message \\"
echo "    --queue-url $SQS_URL \\"
echo "    --message-body '{\"event\": \"test\", \"file\": \"data.csv\"}'"
echo ""
echo "  # Test S3→SNS→SQS chain (sqs_triggered_pipeline):"
echo "  echo 'id,name' > /tmp/test.csv"
echo "  aws s3 cp /tmp/test.csv s3://$S3_BUCKET_NAME/events/test.csv"
echo ""
echo "  # Test S3-only pipeline (s3_file_arrival_pipeline — trigger DAG first):"
echo "  echo 'id,name' > /tmp/test.csv"
echo "  aws s3 cp /tmp/test.csv s3://$S3_BUCKET_NAME/incoming/test.csv"
echo ""
echo "  Then watch localhost:8080 for the DAG to trigger!"
echo "============================================="
