data "aws_iam_policy_document" "flink_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["kinesisanalytics.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "flink_execution_role" {
  name               = "${var.environment}-flink-lakehouse-execution-role"
  assume_role_policy = data.aws_iam_policy_document.flink_assume_role.json

  tags = {
    Environment = var.environment
  }
}

data "aws_iam_policy_document" "flink_kinesis_s3" {
  statement {
    sid    = "KinesisAccess"
    effect = "Allow"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards"
    ]
    resources = [var.kinesis_stream_arn]
  }

  statement {
    sid    = "CloudWatchLogsAccess"
    effect = "Allow"
    actions = [
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }

  statement {
    sid    = "S3ArtifactsAccess"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetBucketLocation"
    ]
    resources = [
      var.datalake_bucket_arn,
      "${var.datalake_bucket_arn}/*"
    ]
  }
}

resource "aws_iam_role_policy" "flink_base_policy" {
  name   = "${var.environment}-flink-base-policy"
  role   = aws_iam_role.flink_execution_role.id
  policy = data.aws_iam_policy_document.flink_kinesis_s3.json
}
