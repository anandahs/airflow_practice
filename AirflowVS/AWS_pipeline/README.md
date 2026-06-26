# AWS Pipeline

## Setup

Before running this project, create a `.env` file in this directory with the following environment variables:

```
AIRFLOW_UID=50000
aws_access_key_id=<your-aws-access-key-id>
aws_secret_access_key=<your-aws-secret-access-key>
```

`.env` is gitignored and should never be committed.

## Prerequisites for Glue IAM setup

If you want to run AWS Glue from this project, make sure your AWS CLI is configured and you have permission to create IAM roles and attach policies.

### 1. Configure AWS CLI

If you have not configured AWS CLI yet, run:

```bash
aws configure
```

You can also use an existing profile:

```bash
export AWS_PROFILE=<your-profile-name>
```

### 2. Create the Glue role

Run the helper script to create an IAM role named `sparkaccess` and attach the managed policies needed for Glue to work with S3, CloudWatch, and Glue itself:

```bash
chmod +x scripts/manage_glue_role.sh
./scripts/manage_glue_role.sh create
```

### 3. Delete the role when you are done

```bash
./scripts/manage_glue_role.sh destroy
```

## What this script does

The script creates an IAM role for AWS Glue and attaches the permissions Glue needs to run jobs successfully.

### IAM role

An IAM role is an identity that AWS services or users can assume temporarily. In this case, the role is named `sparkaccess` and is intended for AWS Glue.

### Trust policy

The script creates a trust policy that allows your AWS account to assume the role, and also allows the Glue service to assume it:

- Trust policy = who is allowed to use the role
- In this case, your account and Glue are both allowed to assume the role

If you want to restrict this further to a specific IAM user or role, set `TRUST_PRINCIPAL_ARN` before running the script:

```bash
TRUST_PRINCIPAL_ARN="arn:aws:iam::<account-id>:user/your-name" ./scripts/manage_glue_role.sh create
```

### Permission policies

The script attaches managed policies that give the role permissions to:

- run Glue jobs and crawlers: `AWSGlueServiceRole`
- read and write data in S3: `AmazonS3FullAccess`
- write logs to CloudWatch: `CloudWatchLogsFullAccess`
- use Glue console features: `AWSGlueConsoleFullAccess`

### Why this is needed

Glue needs permissions to access your data in S3, create logs, and manage Glue resources. The role centralizes those permissions so Glue can operate without embedding long-lived credentials directly in your workflow.

