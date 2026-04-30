#!/bin/bash
cd ~/airflow_practice/my-airflow-project

AWS_ACCESS_KEY=$(aws configure get aws_access_key_id)
AWS_SECRET_KEY=$(aws configure get aws_secret_access_key)
AWS_REGION=$(aws configure get region)

astro dev run connections add aws_default \
  --conn-type aws \
  --conn-extra "{\"aws_access_key_id\": \"$AWS_ACCESS_KEY\", \"aws_secret_access_key\": \"$AWS_SECRET_KEY\", \"region_name\": \"$AWS_REGION\"}"

echo "✅ aws_default connection added!"
astro dev run connections get aws_default
