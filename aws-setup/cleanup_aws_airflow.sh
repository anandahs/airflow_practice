#!/bin/bash
# =============================================================================
# AWS Cleanup Script for Airflow Event-Driven DAGs
# Destroys: SQS Queue, S3 Bucket, SNS Topic, SNS→SQS Subscription
# Usage: cd my-airflow-project && bash cleanup_aws_airflow.sh
# =============================================================================

set -e

# ── Config — must match setup_aws_airflow.sh ──────────────────────────────────
REGION="us-east-1"
SQS_QUEUE_NAME="airflow-events"
S3_BUCKET_NAME="airflow-demo-shiva"
SNS_TOPIC_NAME="airflow-notifications"
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo "============================================="
echo "  Airflow AWS Cleanup"
echo "  ⚠️  This will DELETE all resources!"
echo "============================================="
echo ""

# ── Confirm before destroying ─────────────────────────────────────────────────
read -p "Are you sure you want to delete all resources? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
  echo "❌ Aborted. Nothing was deleted."
  exit 0
fi
echo ""

# ── Get AWS Account ID ────────────────────────────────────────────────────────
echo "🔍 Fetching AWS Account ID..."
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
SQS_URL="https://sqs.$REGION.amazonaws.com/$ACCOUNT_ID/$SQS_QUEUE_NAME"
SNS_ARN="arn:aws:sns:$REGION:$ACCOUNT_ID:$SNS_TOPIC_NAME"
SQS_ARN="arn:aws:sqs:$REGION:$ACCOUNT_ID:$SQS_QUEUE_NAME"
echo "✅ Account ID: $ACCOUNT_ID"
echo ""

# ── Step 1: Remove S3 Event Notification ─────────────────────────────────────
echo "📡 Step 1: Removing S3 event notifications..."
aws s3api put-bucket-notification-configuration \
  --bucket "$S3_BUCKET_NAME" \
  --notification-configuration "{}" \
  --region "$REGION" 2>/dev/null \
  && echo "✅ S3 notifications removed" \
  || echo "⚠️  S3 bucket not found or already clean, skipping..."
echo ""

# ── Step 2: Unsubscribe SQS from SNS ─────────────────────────────────────────
echo "🔗 Step 2: Removing SNS subscriptions..."
SUBSCRIPTIONS=$(aws sns list-subscriptions-by-topic \
  --topic-arn "$SNS_ARN" \
  --region "$REGION" \
  --query 'Subscriptions[*].SubscriptionArn' \
  --output text 2>/dev/null || echo "")

if [ -n "$SUBSCRIPTIONS" ]; then
  for SUB_ARN in $SUBSCRIPTIONS; do
    if [ "$SUB_ARN" != "PendingConfirmation" ]; then
      aws sns unsubscribe \
        --subscription-arn "$SUB_ARN" \
        --region "$REGION" 2>/dev/null \
        && echo "✅ Unsubscribed: $SUB_ARN" \
        || echo "⚠️  Could not unsubscribe: $SUB_ARN"
    fi
  done
else
  echo "⚠️  No subscriptions found, skipping..."
fi
echo ""

# ── Step 3: Delete SNS Topic ──────────────────────────────────────────────────
echo "📣 Step 3: Deleting SNS topic: $SNS_TOPIC_NAME..."
aws sns delete-topic \
  --topic-arn "$SNS_ARN" \
  --region "$REGION" 2>/dev/null \
  && echo "✅ SNS topic deleted" \
  || echo "⚠️  SNS topic not found, skipping..."
echo ""

# ── Step 4: Delete SQS Queue ──────────────────────────────────────────────────
echo "📬 Step 4: Deleting SQS queue: $SQS_QUEUE_NAME..."
aws sqs delete-queue \
  --queue-url "$SQS_URL" \
  --region "$REGION" 2>/dev/null \
  && echo "✅ SQS queue deleted" \
  || echo "⚠️  SQS queue not found, skipping..."
echo ""

# ── Step 5: Empty S3 Bucket ───────────────────────────────────────────────────
echo "🪣  Step 5: Emptying S3 bucket: $S3_BUCKET_NAME..."
# Delete all objects
aws s3 rm "s3://$S3_BUCKET_NAME" --recursive --region "$REGION" 2>/dev/null \
  && echo "✅ All objects deleted" \
  || echo "⚠️  Could not empty bucket or already empty, skipping..."

# Delete all object versions (if versioning was enabled)
VERSIONS=$(aws s3api list-object-versions \
  --bucket "$S3_BUCKET_NAME" \
  --region "$REGION" \
  --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
  --output json 2>/dev/null || echo "{}")

if [ "$VERSIONS" != "{}" ] && [ "$VERSIONS" != "null" ]; then
  aws s3api delete-objects \
    --bucket "$S3_BUCKET_NAME" \
    --delete "$VERSIONS" \
    --region "$REGION" > /dev/null 2>/dev/null \
    && echo "✅ Object versions deleted" \
    || echo "⚠️  No versions to delete"
fi

# Delete all delete markers
MARKERS=$(aws s3api list-object-versions \
  --bucket "$S3_BUCKET_NAME" \
  --region "$REGION" \
  --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' \
  --output json 2>/dev/null || echo "{}")

if [ "$MARKERS" != "{}" ] && [ "$MARKERS" != "null" ]; then
  aws s3api delete-objects \
    --bucket "$S3_BUCKET_NAME" \
    --delete "$MARKERS" \
    --region "$REGION" > /dev/null 2>/dev/null \
    && echo "✅ Delete markers removed" \
    || echo "⚠️  No delete markers to remove"
fi
echo ""

# ── Step 6: Delete S3 Bucket ──────────────────────────────────────────────────
echo "🗑️  Step 6: Deleting S3 bucket: $S3_BUCKET_NAME..."
aws s3 rb "s3://$S3_BUCKET_NAME" --region "$REGION" 2>/dev/null \
  && echo "✅ S3 bucket deleted" \
  || echo "⚠️  S3 bucket not found, skipping..."
echo ""

# ── Step 7: Clean .env file ───────────────────────────────────────────────────
echo "🧹 Step 7: Cleaning .env file..."
if [ -f ".env" ]; then
  # Remove the auto-generated block from .env
  sed -i '' '/# ── AWS Event-Driven DAG Config/,/AWS_ACCOUNT_ID=/d' .env \
    && echo "✅ .env cleaned" \
    || echo "⚠️  Could not clean .env, remove AWS lines manually"
else
  echo "⚠️  No .env file found, skipping..."
fi
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "============================================="
echo "  ✅ Cleanup Complete!"
echo "============================================="
echo ""
echo "  Deleted:"
echo "  ❌ SQS  : $SQS_QUEUE_NAME"
echo "  ❌ S3   : s3://$S3_BUCKET_NAME"
echo "  ❌ SNS  : $SNS_TOPIC_NAME"
echo "  ❌ All SNS subscriptions"
echo "  ❌ All S3 event notifications"
echo ""
echo "  Note: SQS queues take up to 60 seconds"
echo "  to fully delete on AWS side. If you"
echo "  recreate with the same name, wait 60s first."
echo "============================================="
