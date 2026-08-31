resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "datalake" {
  bucket        = "${var.environment}-datalake-${random_id.bucket_suffix.hex}"
  force_destroy = true

  tags = {
    Name        = "${var.environment}-datalake"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
