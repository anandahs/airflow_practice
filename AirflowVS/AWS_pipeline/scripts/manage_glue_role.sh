#!/usr/bin/env bash
set -euo pipefail

ROLE_NAME="${ROLE_NAME:-sparkaccess}"
TRUST_PRINCIPAL_ARN="${TRUST_PRINCIPAL_ARN:-}"
ACCOUNT_ID="${ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)}"
POLICIES=(
  "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  "arn:aws:iam::aws:policy/AmazonS3FullAccess"
  "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
  "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
)

usage() {
  cat <<EOF
Usage: $(basename "$0") create|destroy

Creates or deletes the IAM role named '${ROLE_NAME}' for AWS Glue.

Examples:
  ./scripts/manage_glue_role.sh create
  ./scripts/manage_glue_role.sh destroy
EOF
}

create_role() {
  local trust_policy_file
  local principal_arn
  trust_policy_file="$(mktemp)"

  if [[ -z "$ACCOUNT_ID" ]]; then
    echo "Unable to determine the AWS account ID from sts:get-caller-identity." >&2
    exit 1
  fi

  principal_arn="${TRUST_PRINCIPAL_ARN:-arn:aws:iam::${ACCOUNT_ID}:root}"

  cat > "$trust_policy_file" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowAccountToAssumeRole",
      "Effect": "Allow",
      "Principal": {
        "AWS": "${principal_arn}"
      },
      "Action": "sts:AssumeRole"
    },
    {
      "Sid": "AllowGlueServiceToAssumeRole",
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON

  if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
    echo "Role '$ROLE_NAME' already exists."
  else
    aws iam create-role \
      --role-name "$ROLE_NAME" \
      --assume-role-policy-document "file://$trust_policy_file" \
      --description "Allows Glue to call AWS services on your behalf." >/dev/null
    echo "Created role '$ROLE_NAME'."
  fi

  for policy_arn in "${POLICIES[@]}"; do
    attached_policy_output=$(aws iam list-attached-role-policies \
      --role-name "$ROLE_NAME" \
      --query "AttachedPolicies[?PolicyArn=='${policy_arn}'].PolicyArn" \
      --output text 2>/dev/null || true)

    if [[ -n "$attached_policy_output" ]]; then
      echo "Policy already attached: $policy_arn"
    else
      aws iam attach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn "$policy_arn" >/dev/null
      echo "Attached policy: $policy_arn"
    fi
  done

  rm -f "$trust_policy_file"
}

destroy_role() {
  if ! aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
    echo "Role '$ROLE_NAME' does not exist."
    exit 0
  fi

  while IFS= read -r policy_arn; do
    if [[ -n "$policy_arn" ]]; then
      aws iam detach-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-arn "$policy_arn" >/dev/null
      echo "Detached policy: $policy_arn"
    fi
  done < <(aws iam list-attached-role-policies \
    --role-name "$ROLE_NAME" \
    --query 'AttachedPolicies[].PolicyArn' \
    --output text | tr -s '[:space:]' '\n')

  while IFS= read -r inline_policy_name; do
    if [[ -n "$inline_policy_name" ]]; then
      aws iam delete-role-policy \
        --role-name "$ROLE_NAME" \
        --policy-name "$inline_policy_name" >/dev/null
      echo "Deleted inline policy: $inline_policy_name"
    fi
  done < <(aws iam list-role-policies \
    --role-name "$ROLE_NAME" \
    --query 'PolicyNames[]' \
    --output text | tr -s '[:space:]' '\n')

  aws iam delete-role --role-name "$ROLE_NAME" >/dev/null
  echo "Deleted role '$ROLE_NAME'."
}

case "${1:-}" in
  create)
    create_role
    ;;
  destroy)
    destroy_role
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
