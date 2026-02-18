# ============================================================
# CryptoLake — Local Environment
# ============================================================
# En local, la infraestructura se gestiona con Docker Compose.
# MinIO simula S3, no necesita Terraform.
#
# Este archivo existe como documentación y para demostrar que
# la estructura de Terraform soporta múltiples entornos.
#
# Buckets locales (creados por minio-init en docker-compose):
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
