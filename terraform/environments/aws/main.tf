# ============================================================
# CryptoLake — AWS Environment
# ============================================================
# Despliega el storage layer en AWS S3.
# Ejecutar:
#   cd terraform/environments/aws
#   terraform init
#   terraform plan
#   terraform apply
# ============================================================

terraform {
  required_version = ">= 1.8"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # En producción: backend remoto en S3
  # backend "s3" {
  #   bucket = "cryptolake-terraform-state"
  #   key    = "aws/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "staging"
}

# ── Módulo de Storage ───────────────────────────────────────

module "storage" {
  source      = "../../modules/storage"
  environment = var.environment
}

# ── Outputs ─────────────────────────────────────────────────

output "bronze_bucket" {
  value = module.storage.bronze_bucket_name
}

output "silver_bucket" {
  value = module.storage.silver_bucket_name
}

output "gold_bucket" {
  value = module.storage.gold_bucket_name
}
