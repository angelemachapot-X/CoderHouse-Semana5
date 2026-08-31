terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Configurar con los valores generados por ./bootstrap/bootstrap-backend.sh
  backend "s3" {
    bucket       = "coderhouse-chapot-angel-de-2026"
    key          = "dev/terraform.tfstate"
    region       = "us-east-2"
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = var.region
}

module "network" {
  source      = "../../modules/network"
  environment = var.environment
  region      = var.region
  vpc_cidr    = var.vpc_cidr
}

module "ingestion" {
  source              = "../../modules/ingestion"
  environment         = var.environment
  shard_count         = var.shard_count
  datalake_bucket_arn = module.network.datalake_bucket_arn
}

module "identity" {
  source              = "../../modules/identity"
  environment         = var.environment
  datalake_bucket_arn = module.network.datalake_bucket_arn
  kinesis_stream_arn  = module.ingestion.kinesis_stream_arn
}
