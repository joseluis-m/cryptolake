# ============================================================
# CryptoLake — Storage Module
# ============================================================
# Creates S3 buckets for the Lakehouse layers (Bronze, Silver, Gold).
# Includes lifecycle policies and versioning for data protection.
# ============================================================

variable "environment" {
  type        = string
  description = "Environment name (local, staging, production)"
}

variable "project_name" {
  type    = string
  default = "cryptolake"
}

# -- S3 Buckets ----------------------------------------------

resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-${var.environment}-bronze"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "bronze"
  }
}

resource "aws_s3_bucket" "silver" {
  bucket = "${var.project_name}-${var.environment}-silver"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "silver"
  }
}

resource "aws_s3_bucket" "gold" {
  bucket = "${var.project_name}-${var.environment}-gold"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "gold"
  }
}

# -- Lifecycle: archive old Bronze data to Glacier -----------

resource "aws_s3_bucket_lifecycle_configuration" "bronze_lifecycle" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# -- Versioning: protect raw data in Bronze ------------------

resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id

  versioning_configuration {
    status = "Enabled"
  }
}

# -- Outputs -------------------------------------------------

output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze.id
}

output "silver_bucket_name" {
  value = aws_s3_bucket.silver.id
}

output "gold_bucket_name" {
  value = aws_s3_bucket.gold.id
}
