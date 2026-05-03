#!/bin/bash
# =============================================================================
# EC2 + SSM Cleanup Script
# Destroys: EC2 instances, IAM role/profile, Security Group
# Usage: bash cleanup_ec2_ssm.sh
# =============================================================================

set -e

REGION="us-east-1"
INSTANCE_NAME="airflow-ssm-worker"
SECURITY_GROUP_NAME="airflow-ssm-sg"
IAM_ROLE_NAME="airflow-ssm-ec2-role"
IAM_PROFILE_NAME="airflow-ssm-ec2-profile"

echo ""
echo "============================================="
echo "  EC2 + SSM Cleanup"
echo "  ⚠️  This will DELETE all resources!"
echo "============================================="
echo ""

read -p "Are you sure? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
  echo "❌ Aborted."
  exit 0
fi
echo ""

# ── Step 1: Terminate EC2 instances ──────────────────────────────────────────
echo "🖥️  Step 1: Terminating EC2 instances tagged as $INSTANCE_NAME..."
INSTANCE_IDS=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$INSTANCE_NAME" "Name=instance-state-name,Values=running,stopped,pending" \
  --query 'Reservations[*].Instances[*].InstanceId' \
  --output text --region "$REGION" 2>/dev/null || echo "")

if [ -n "$INSTANCE_IDS" ]; then
  aws ec2 terminate-instances \
    --instance-ids $INSTANCE_IDS \
    --region "$REGION" > /dev/null
  echo "✅ Terminating: $INSTANCE_IDS"
  echo "   Waiting for termination..."
  aws ec2 wait instance-terminated \
    --instance-ids $INSTANCE_IDS \
    --region "$REGION"
  echo "✅ Instances terminated"
else
  echo "⚠️  No instances found, skipping..."
fi
echo ""

# ── Step 2: Delete IAM role ───────────────────────────────────────────────────
echo "🔐 Step 2: Cleaning up IAM role..."

# Detach policies first
aws iam detach-role-policy \
  --role-name "$IAM_ROLE_NAME" \
  --policy-arn "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore" 2>/dev/null \
  && echo "✅ SSM policy detached" || echo "⚠️  SSM policy not attached, skipping..."

aws iam detach-role-policy \
  --role-name "$IAM_ROLE_NAME" \
  --policy-arn "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess" 2>/dev/null \
  && echo "✅ S3 policy detached" || echo "⚠️  S3 policy not attached, skipping..."

# Remove role from instance profile
aws iam remove-role-from-instance-profile \
  --instance-profile-name "$IAM_PROFILE_NAME" \
  --role-name "$IAM_ROLE_NAME" 2>/dev/null \
  && echo "✅ Role removed from instance profile" || echo "⚠️  Skipping..."

# Delete instance profile
aws iam delete-instance-profile \
  --instance-profile-name "$IAM_PROFILE_NAME" 2>/dev/null \
  && echo "✅ Instance profile deleted" || echo "⚠️  Instance profile not found, skipping..."

# Delete role
aws iam delete-role \
  --role-name "$IAM_ROLE_NAME" 2>/dev/null \
  && echo "✅ IAM role deleted" || echo "⚠️  IAM role not found, skipping..."
echo ""

# ── Step 3: Delete Security Group ────────────────────────────────────────────
echo "🔒 Step 3: Deleting security group..."
SG_ID=$(aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" \
  --query 'SecurityGroups[0].GroupId' \
  --output text --region "$REGION" 2>/dev/null || echo "None")

if [ "$SG_ID" != "None" ] && [ -n "$SG_ID" ]; then
  aws ec2 delete-security-group \
    --group-id "$SG_ID" \
    --region "$REGION" 2>/dev/null \
    && echo "✅ Security group deleted" \
    || echo "⚠️  Could not delete security group (may still be in use)"
else
  echo "⚠️  Security group not found, skipping..."
fi
echo ""

# ── Step 4: Clean .env ────────────────────────────────────────────────────────
echo "🧹 Step 4: Cleaning .env..."
ENV_FILE="../my-airflow-project/.env"
if [ -f "$ENV_FILE" ]; then
  sed -i '' '/# ── EC2 SSM Config/,/EC2_SCRIPT_PATH=/d' "$ENV_FILE" 2>/dev/null \
    && echo "✅ .env cleaned" || echo "⚠️  Could not clean .env"
else
  echo "⚠️  .env not found, skipping..."
fi
echo ""

echo "============================================="
echo "  ✅ Cleanup Complete!"
echo "============================================="
echo ""
echo "  Deleted:"
echo "  ❌ EC2 instances tagged: $INSTANCE_NAME"
echo "  ❌ IAM Role: $IAM_ROLE_NAME"
echo "  ❌ IAM Profile: $IAM_PROFILE_NAME"
echo "  ❌ Security Group: $SECURITY_GROUP_NAME"
echo "============================================="
