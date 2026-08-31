#!/bin/bash
set -e

REGION="${AWS_REGION:-us-east-1}"
BUCKET_NAME="${1:?Uso: ./bootstrap-backend.sh <nombre-bucket-state> [--legacy-dynamodb <nombre-tabla>]}"
DYNAMO_TABLE=""

if [ "$2" = "--legacy-dynamodb" ]; then
  DYNAMO_TABLE="${3:?Falta el nombre de la tabla después de --legacy-dynamodb}"
fi

echo "Creando bucket de state: $BUCKET_NAME en $REGION..."
if [ "$REGION" = "us-east-1" ]; then
  aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION"
else
  aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION"
fi

aws s3api put-bucket-versioning --bucket "$BUCKET_NAME" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption --bucket "$BUCKET_NAME" \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

aws s3api put-public-access-block --bucket "$BUCKET_NAME" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

if [ -n "$DYNAMO_TABLE" ]; then
  echo "Creando tabla DynamoDB de locking (modo legacy): $DYNAMO_TABLE..."
  aws dynamodb create-table \
    --table-name "$DYNAMO_TABLE" \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region "$REGION"
fi

echo ""
echo "=========================================================="
echo "Bucket de estado remoto configurado con éxito."
echo "Ahora actualiza infra/environments/dev/main.tf con:"
echo "  bucket = "$BUCKET_NAME""
echo "  region = "$REGION""
if [ -n "$DYNAMO_TABLE" ]; then
  echo "  dynamodb_table = "$DYNAMO_TABLE"   (en vez de use_lockfile = true)"
else
  echo "  use_lockfile = true"
fi
echo "=========================================================="
