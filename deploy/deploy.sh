#!/bin/bash
# Deploy Wemby pipeline to AWS Lambda + EventBridge
# Prerequisites: AWS CLI configured, Docker running
# Usage: bash deploy/deploy.sh

set -e

AWS_REGION="us-east-1"
AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ECR_REPO="wemby-pipeline"
LAMBDA_NAME="wemby-pipeline"
IMAGE_URI="$AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:latest"

echo "==> Building Docker image..."
docker build -t $ECR_REPO .

echo "==> Pushing to ECR..."
aws ecr get-login-password --region $AWS_REGION \
  | docker login --username AWS --password-stdin "$AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com"

aws ecr describe-repositories --repository-names $ECR_REPO --region $AWS_REGION 2>/dev/null \
  || aws ecr create-repository --repository-name $ECR_REPO --region $AWS_REGION

docker tag $ECR_REPO:latest $IMAGE_URI
docker push $IMAGE_URI

echo "==> Deploying Lambda..."
aws lambda get-function --function-name $LAMBDA_NAME --region $AWS_REGION 2>/dev/null \
  && aws lambda update-function-code \
       --function-name $LAMBDA_NAME \
       --image-uri $IMAGE_URI \
       --region $AWS_REGION \
  || aws lambda create-function \
       --function-name $LAMBDA_NAME \
       --package-type Image \
       --code ImageUri=$IMAGE_URI \
       --role "arn:aws:iam::$AWS_ACCOUNT:role/wemby-lambda-role" \
       --timeout 300 \
       --memory-size 1024 \
       --environment "Variables={
         ODDS_API_KEY=$ODDS_API_KEY,
         FAL_KEY=$FAL_KEY,
         DATA_DIR=/tmp/data
       }" \
       --region $AWS_REGION

LAMBDA_ARN=$(aws lambda get-function --function-name $LAMBDA_NAME \
  --region $AWS_REGION --query Configuration.FunctionArn --output text)

echo "==> Creating EventBridge rules..."

# Pipeline — 9am ET (13:00 UTC)
aws events put-rule \
  --name wemby-pipeline-9am-et \
  --schedule-expression "cron(0 13 * * ? *)" \
  --state ENABLED \
  --region $AWS_REGION

aws events put-targets \
  --rule wemby-pipeline-9am-et \
  --targets "Id=pipeline,Arn=$LAMBDA_ARN,Input={\"job\":\"pipeline\"}" \
  --region $AWS_REGION

# Results — 11:59pm MT (05:59 UTC)
aws events put-rule \
  --name wemby-results-1159pm-mt \
  --schedule-expression "cron(59 5 * * ? *)" \
  --state ENABLED \
  --region $AWS_REGION

aws events put-targets \
  --rule wemby-results-1159pm-mt \
  --targets "Id=results,Arn=$LAMBDA_ARN,Input={\"job\":\"results\"}" \
  --region $AWS_REGION

# Allow EventBridge to invoke Lambda
aws lambda add-permission \
  --function-name $LAMBDA_NAME \
  --statement-id eventbridge-pipeline \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:$AWS_REGION:$AWS_ACCOUNT:rule/wemby-*" \
  --region $AWS_REGION 2>/dev/null || true

echo ""
echo "Deploy complete."
echo "  Lambda: $LAMBDA_ARN"
echo "  Pipeline fires: 9am ET daily"
echo "  Results fires:  11:59pm MT daily"
