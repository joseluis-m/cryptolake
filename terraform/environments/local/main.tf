# ============================================================
# CryptoLake — Local Environment
# ============================================================
# In local development, infrastructure is managed by Docker Compose.
# MinIO provides S3-compatible storage without Terraform.
#
# This file exists to document the multi-environment structure
# and demonstrate that Terraform supports environment parity.
#
# Local buckets (created by minio-init in docker-compose):
#   - cryptolake-bronze
#   - cryptolake-silver
#   - cryptolake-gold
#   - cryptolake-checkpoints
# ============================================================

terraform {
  required_version = ">= 1.8"
}

output "info" {
  value = "Local environment uses Docker Compose + MinIO. No Terraform resources needed."
}
