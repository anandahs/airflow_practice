#!/bin/bash
# =============================================================================
# EC2 + SSM + SSH Key Setup Script
# Creates: IAM Role, EC2 instance with SSM agent, Key Pair, Security Group
# Supports BOTH SSM (org pattern) and SSH key (debug/fallback)
# Usage: cd aws-setup && bash setup_ec2_ssm.sh
# =============================================================================

set -e

# ── Config ────────────────────────────────────────────────────────────────────
REGION="us-east-1"
INSTANCE_NAME="airflow-ssm-worker"
INSTANCE_TYPE="t3.micro"
AMI_ID="ami-0c02fb55956c7d316"        # Amazon Linux 2 us-east-1
SECURITY_GROUP_NAME="airflow-ssm-sg"
IAM_ROLE_NAME="airflow-ssm-ec2-role"
IAM_PROFILE_NAME="airflow-ssm-ec2-profile"
KEY_NAME="airflow-ec2-key"
KEY_PATH="$HOME/.ssh/${KEY_NAME}.pem"
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo "============================================="
echo "  EC2 + SSM + SSH Key Setup"
echo "  Mode: SSM (org pattern) + SSH (debug)"
echo "============================================="
echo ""

# ── Get Account ID + Your IP ──────────────────────────────────────────────────
echo "🔍 Fetching AWS Account ID and your public IP..."
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
MY_IP=$(curl -s https://checkip.amazonaws.com)
echo "✅ Account ID : $ACCOUNT_ID"
echo "✅ Your IP    : $MY_IP (SSH will be locked to this IP)"
echo ""

# ── Step 1: Create SSH Key Pair ───────────────────────────────────────────────
echo "🔑 Step 1: Creating SSH Key Pair: $KEY_NAME..."

KEY_EXISTS=$(aws ec2 describe-key-pairs \
  --key-names "$KEY_NAME" \
  --region "$REGION" \
  --query 'KeyPairs[0].KeyName' \
  --output text 2>/dev/null || echo "")

if [ -z "$KEY_EXISTS" ] || [ "$KEY_EXISTS" == "None" ]; then
  aws ec2 create-key-pair \
    --key-name "$KEY_NAME" \
    --region "$REGION" \
    --query 'KeyMaterial' \
    --output text > "$KEY_PATH"
  chmod 400 "$KEY_PATH"
  echo "✅ Key saved to  : $KEY_PATH"
  echo "✅ Permissions   : 400 (read-only)"
else
  echo "⚠️  Key already exists in AWS."
  [ -f "$KEY_PATH" ] \
    && echo "✅ Local key found: $KEY_PATH" \
    || echo "❌ Key missing locally! Delete from AWS and re-run:"
    echo "   aws ec2 delete-key-pair --key-name $KEY_NAME --region $REGION"
fi
echo ""

# ── Step 2: Create IAM Role ───────────────────────────────────────────────────
echo "🔐 Step 2: Creating IAM Role: $IAM_ROLE_NAME..."

ROLE_EXISTS=$(aws iam get-role --role-name "$IAM_ROLE_NAME" \
  --query 'Role.RoleName' --output text 2>/dev/null || echo "")

if [ -z "$ROLE_EXISTS" ]; then
  TRUST_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ec2.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}
EOF
)
  aws iam create-role \
    --role-name "$IAM_ROLE_NAME" \
    --assume-role-policy-document "$TRUST_POLICY" \
    --description "EC2 role for Airflow SSM pipeline" > /dev/null
  echo "✅ IAM Role created"
else
  echo "⚠️  IAM Role already exists, skipping..."
fi

aws iam attach-role-policy --role-name "$IAM_ROLE_NAME" \
  --policy-arn "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore" 2>/dev/null \
  && echo "✅ AmazonSSMManagedInstanceCore attached" \
  || echo "⚠️  Already attached"

aws iam attach-role-policy --role-name "$IAM_ROLE_NAME" \
  --policy-arn "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess" 2>/dev/null \
  && echo "✅ AmazonS3ReadOnlyAccess attached" \
  || echo "⚠️  Already attached"
echo ""

# ── Step 3: Create Instance Profile ──────────────────────────────────────────
echo "👤 Step 3: Creating Instance Profile..."

PROFILE_EXISTS=$(aws iam get-instance-profile \
  --instance-profile-name "$IAM_PROFILE_NAME" \
  --query 'InstanceProfile.InstanceProfileName' \
  --output text 2>/dev/null || echo "")

if [ -z "$PROFILE_EXISTS" ]; then
  aws iam create-instance-profile \
    --instance-profile-name "$IAM_PROFILE_NAME" > /dev/null
  aws iam add-role-to-instance-profile \
    --instance-profile-name "$IAM_PROFILE_NAME" \
    --role-name "$IAM_ROLE_NAME"
  echo "✅ Instance Profile created"
else
  echo "⚠️  Instance Profile already exists, skipping..."
fi
echo ""

# ── Step 4: Create Security Group ────────────────────────────────────────────
echo "🔒 Step 4: Creating Security Group..."

SG_ID=$(aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=$SECURITY_GROUP_NAME" \
  --query 'SecurityGroups[0].GroupId' \
  --output text --region "$REGION" 2>/dev/null || echo "None")

if [ "$SG_ID" == "None" ] || [ -z "$SG_ID" ]; then
  SG_ID=$(aws ec2 create-security-group \
    --group-name "$SECURITY_GROUP_NAME" \
    --description "Airflow EC2: SSM (no inbound) + SSH (your IP only)" \
    --region "$REGION" \
    --query 'GroupId' \
    --output text)
  echo "✅ Security Group created: $SG_ID"
else
  echo "⚠️  Security Group exists: $SG_ID"
fi

# Add SSH rule for your IP only
SSH_RULE=$(aws ec2 describe-security-groups \
  --group-ids "$SG_ID" \
  --query "SecurityGroups[0].IpPermissions[?FromPort==\`22\`]" \
  --output text --region "$REGION" 2>/dev/null || echo "")

if [ -z "$SSH_RULE" ]; then
  aws ec2 authorize-security-group-ingress \
    --group-id "$SG_ID" \
    --protocol tcp --port 22 \
    --cidr "${MY_IP}/32" \
    --region "$REGION" > /dev/null
  echo "✅ SSH port 22 allowed from: $MY_IP only"
else
  echo "⚠️  SSH rule already exists, skipping..."
fi
echo "   SSM uses outbound HTTPS — no inbound rule needed ✅"
echo ""

# ── Step 5: Prepare User Data ────────────────────────────────────────────────
echo "📝 Step 5: Preparing EC2 startup script..."

cat > /tmp/ec2_user_data.sh <<'USERDATA'
#!/bin/bash
yum update -y
yum install -y python3 python3-pip aws-cli
mkdir -p /home/ec2-user/app

cat > /home/ec2-user/app/process_file.sh << 'SCRIPT'
#!/bin/bash
# process_file.sh — triggered by Airflow via SSM or SSH
# Usage: bash process_file.sh --bucket <bucket> --file <key>

BUCKET=""
FILE_KEY=""

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --bucket) BUCKET="$2"; shift ;;
        --file)   FILE_KEY="$2"; shift ;;
        *) echo "Unknown: $1" ;;
    esac
    shift
done

echo "================================================"
echo "  EC2 Processing Started  —  $(date)"
echo "================================================"
echo "  Bucket   : $BUCKET"
echo "  File     : $FILE_KEY"
echo "  Host     : $(hostname)"
echo "  Instance : $(curl -s http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null)"
echo "  Method   : ${TRIGGER_METHOD:-ssm}"
echo "================================================"

echo "📥 Downloading from S3..."
aws s3 cp "s3://$BUCKET/$FILE_KEY" /tmp/input_file.csv
[ $? -ne 0 ] && echo "❌ Download failed" && exit 1

echo "⚙️  Processing..."
LINE_COUNT=$(wc -l < /tmp/input_file.csv)
echo "   Lines: $LINE_COUNT"
sleep 3

echo "$(date),processed,$FILE_KEY,$LINE_COUNT" >> /tmp/processing_log.csv
echo "✅ Done! $LINE_COUNT lines processed."
echo "================================================"
SCRIPT

chmod +x /home/ec2-user/app/process_file.sh
chown -R ec2-user:ec2-user /home/ec2-user/app
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent
echo "EC2 ready!" > /tmp/setup_complete.txt
USERDATA

echo "✅ User data prepared"
echo ""

# ── Step 6: Launch EC2 ───────────────────────────────────────────────────────
echo "🚀 Step 6: Launching EC2 instance..."
echo "   Waiting 15s for IAM profile propagation..."
sleep 15

INSTANCE_ID=$(aws ec2 run-instances \
  --image-id "$AMI_ID" \
  --instance-type "$INSTANCE_TYPE" \
  --key-name "$KEY_NAME" \
  --security-group-ids "$SG_ID" \
  --iam-instance-profile Name="$IAM_PROFILE_NAME" \
  --user-data "file:///tmp/ec2_user_data.sh" \
  --tag-specifications \
    "ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_NAME},{Key=Environment,Value=airflow-dev},{Key=ManagedBy,Value=airflow-ssm}]" \
  --region "$REGION" \
  --query 'Instances[0].InstanceId' \
  --output text)

echo "✅ Launched: $INSTANCE_ID"
echo ""

# ── Step 7: Wait for running state ───────────────────────────────────────────
echo "⏳ Step 7: Waiting for instance to start..."
aws ec2 wait instance-running \
  --instance-ids "$INSTANCE_ID" --region "$REGION"

PUBLIC_IP=$(aws ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' \
  --output text --region "$REGION")

PUBLIC_DNS=$(aws ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicDnsName' \
  --output text --region "$REGION")

echo "✅ Running!"
echo "   Public IP  : $PUBLIC_IP"
echo "   Public DNS : $PUBLIC_DNS"
echo ""

# ── Step 8: Wait for SSM registration ────────────────────────────────────────
echo "⏳ Step 8: Waiting for SSM agent (up to 3 mins)..."
SSM_REGISTERED=false
for i in {1..18}; do
  SSM_STATUS=$(aws ssm describe-instance-information \
    --filters "Key=InstanceIds,Values=$INSTANCE_ID" \
    --query 'InstanceInformationList[0].PingStatus' \
    --output text --region "$REGION" 2>/dev/null || echo "")

  if [ "$SSM_STATUS" == "Online" ]; then
    SSM_REGISTERED=true
    echo "✅ SSM Agent Online!"
    break
  fi
  echo "   Attempt $i/18 — ${SSM_STATUS:-not registered yet}. Waiting 10s..."
  sleep 10
done
[ "$SSM_REGISTERED" = false ] && echo "⚠️  SSM not ready yet — check in a few minutes"
echo ""

# ── Step 9: Test SSM ──────────────────────────────────────────────────────────
if [ "$SSM_REGISTERED" = true ]; then
  echo "🧪 Step 9: Testing SSM..."
  CMD_ID=$(aws ssm send-command \
    --instance-ids "$INSTANCE_ID" \
    --document-name "AWS-RunShellScript" \
    --parameters 'commands=["echo SSM_OK && curl -s http://169.254.169.254/latest/meta-data/instance-id"]' \
    --region "$REGION" \
    --query 'Command.CommandId' --output text)
  sleep 8
  SSM_OUT=$(aws ssm get-command-invocation \
    --command-id "$CMD_ID" --instance-id "$INSTANCE_ID" \
    --region "$REGION" --query 'StandardOutputContent' \
    --output text 2>/dev/null || echo "pending")
  echo "✅ SSM Output: $SSM_OUT"
else
  echo "⏭️  Step 9: Skipping SSM test"
fi
echo ""

# ── Step 10: Test SSH ─────────────────────────────────────────────────────────
echo "🧪 Step 10: Testing SSH (waiting 30s for sshd)..."
sleep 30
SSH_OUT=$(ssh -i "$KEY_PATH" -o StrictHostKeyChecking=no \
  -o ConnectTimeout=15 ec2-user@"$PUBLIC_IP" \
  "echo SSH_OK && hostname" 2>/dev/null || echo "not_ready")

if [[ "$SSH_OUT" == *"SSH_OK"* ]]; then
  echo "✅ SSH Output: $SSH_OUT"
else
  echo "⚠️  SSH not ready yet. Try manually:"
  echo "   ssh -i $KEY_PATH ec2-user@$PUBLIC_IP"
fi
echo ""

# ── Step 11: Write .env ───────────────────────────────────────────────────────
echo "📝 Step 11: Writing to .env..."
ENV_FILE="../my-airflow-project/.env"
if [ -f "$ENV_FILE" ]; then
  sed -i '' '/# ── EC2 SSM Config/,/EC2_SSH_USER=/d' "$ENV_FILE" 2>/dev/null || true
  cat >> "$ENV_FILE" <<EOF

# ── EC2 SSM Config (auto-generated) ───────────────────
EC2_INSTANCE_IDS=$INSTANCE_ID
EC2_REGION=$REGION
EC2_SCRIPT_PATH=/home/ec2-user/app/process_file.sh
EC2_PUBLIC_IP=$PUBLIC_IP
EC2_PUBLIC_DNS=$PUBLIC_DNS
EC2_KEY_PATH=$KEY_PATH
EC2_SSH_USER=ec2-user
EOF
  echo "✅ Written to $ENV_FILE"
else
  echo "⚠️  .env not found — add manually:"
  echo "   EC2_INSTANCE_IDS=$INSTANCE_ID"
  echo "   EC2_PUBLIC_IP=$PUBLIC_IP"
  echo "   EC2_KEY_PATH=$KEY_PATH"
fi
echo ""

# ── Step 12: VS Code SSH config ───────────────────────────────────────────────
echo "🖥️  Step 12: Adding VS Code Remote SSH config..."
SSH_CONFIG="$HOME/.ssh/config"
sed -i '' '/Host airflow-ec2/,/ServerAliveInterval/d' "$SSH_CONFIG" 2>/dev/null || true
cat >> "$SSH_CONFIG" <<EOF

# Airflow EC2 Dev Instance (auto-generated)
Host airflow-ec2
    HostName $PUBLIC_IP
    User ec2-user
    IdentityFile $KEY_PATH
    StrictHostKeyChecking no
    ServerAliveInterval 60
EOF
echo "✅ VS Code SSH: Cmd+Shift+P → Remote-SSH: Connect to Host → airflow-ec2"
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "============================================="
echo "  ✅ Setup Complete!"
echo "============================================="
echo ""
echo "  Instance ID  : $INSTANCE_ID"
echo "  Public IP    : $PUBLIC_IP"
echo "  Key file     : $KEY_PATH"
echo ""
echo "  ── Option 1: SSM (simulates MWAA, no keys) ──"
echo "  aws ssm send-command \\"
echo "    --instance-ids $INSTANCE_ID \\"
echo "    --document-name AWS-RunShellScript \\"
echo "    --parameters 'commands=[\"bash /home/ec2-user/app/process_file.sh --bucket airflow-demo-shiva --file incoming/test.csv\"]' \\"
echo "    --region $REGION"
echo ""
echo "  ── Option 2: SSH (debug/fallback) ──"
echo "  ssh -i $KEY_PATH ec2-user@$PUBLIC_IP"
echo "  ssh -i ~/.ssh/airflow-ec2-key.pem -o StrictHostKeyChecking=no ec2-user@PUBLIC_IP"
echo ""
echo "  ── Option 3: VS Code Remote SSH ──"
echo "  Cmd+Shift+P → Remote-SSH: Connect to Host → airflow-ec2"
echo "============================================="
