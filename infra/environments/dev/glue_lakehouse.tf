# ── Lakehouse: Iceberg + Glue (Semana 5 — Persistencia Transaccional) ────────

resource "aws_glue_catalog_database" "lakehouse_db" {
  name        = "lakehouse_db"
  description = "Metastore central para las tablas Iceberg del pipeline de streaming"
}

resource "aws_s3_bucket_versioning" "datalake_versioning" {
  bucket = module.network.datalake_bucket_name

  versioning_configuration {
    status = "Enabled"
  }
}

data "aws_iam_policy_document" "flink_glue_access" {
  statement {
    sid    = "GlueCatalogAccess"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:BatchGetPartition"
    ]
    resources = [
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.lakehouse_db.name}",
      "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.lakehouse_db.name}/*"
    ]
  }

  statement {
    sid    = "LakehouseS3Access"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      module.network.datalake_bucket_arn,
      "${module.network.datalake_bucket_arn}/*"
    ]
  }
}

resource "aws_iam_role_policy" "flink_glue_policy" {
  name   = "flink-glue-lakehouse-access"
  role   = module.identity.flink_execution_role_name
  policy = data.aws_iam_policy_document.flink_glue_access.json
}

data "aws_caller_identity" "current" {}

output "lakehouse_db_name" {
  description = "Nombre de la base de datos creada en Glue Catalog"
  value       = aws_glue_catalog_database.lakehouse_db.name
}

output "lakehouse_table_path" {
  description = "URI del Warehouse en S3 para las tablas Iceberg"
  value       = "s3://${module.network.datalake_bucket_name}/lakehouse/"
}

output "datalake_bucket_name" {
  description = "Nombre del bucket S3 de Data Lake"
  value       = module.network.datalake_bucket_name
}

output "kinesis_stream_name" {
  description = "Nombre del Kinesis Data Stream de ingestión"
  value       = module.ingestion.kinesis_stream_name
}

output "flink_app_name" {
  description = "Nombre de la aplicación Managed Flink"
  value       = aws_kinesisanalyticsv2_application.flink_job.name
}
