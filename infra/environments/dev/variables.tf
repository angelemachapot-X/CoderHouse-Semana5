variable "environment" {
  description = "Ambiente de despliegue (e.g. dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "region" {
  description = "Región de AWS para desplegar los recursos"
  type        = string
  default     = "us-east-2"
}

variable "vpc_cidr" {
  description = "CIDR block para la VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "shard_count" {
  description = "Cantidad de shards para el Kinesis Stream de ingestión"
  type        = number
  default     = 2
}
