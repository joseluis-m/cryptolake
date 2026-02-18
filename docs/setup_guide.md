# Setup Guide — CryptoLake

Guía completa para levantar el proyecto desde cero.

## Requisitos previos

| Software | Versión mínima | Verificar con |
|----------|---------------|---------------|
| Docker Desktop | 24+ | `docker --version` |
| Docker Compose | v2+ | `docker compose version` |
| Python | 3.11+ | `python3 --version` |
| Make | cualquiera | `make --version` |
| Git | cualquiera | `git --version` |

**Recursos Docker recomendados**: 6 CPU cores, 8 GB RAM, 40 GB disk.

## Paso 1: Clonar y configurar
```bash
git clone https://github.com/tu-usuario/cryptolake.git
cd cryptolake
cp .env.example .env
```

## Paso 2: Levantar servicios
```bash
make up
```

Esto levanta 12+ contenedores. Espera ~60 segundos a que todos estén healthy.

Verifica con:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

## Paso 3: Ejecutar el pipeline
```bash
# Pipeline completo paso a paso
make init-namespaces    # Crear namespaces Iceberg
make bronze-load        # Ingestar datos de APIs → Bronze
make silver-transform   # Bronze → Silver (limpiar + deduplicar)
make dbt-run            # Silver → Gold (star schema con dbt)
make dbt-test           # Validar modelos dbt
make quality-check      # Quality checks (15+ validaciones)

# O todo junto:
make pipeline
```

## Paso 4: Verificar resultados
```bash
# API REST
curl http://localhost:8000/api/v1/health
curl http://localhost:8000/api/v1/analytics/market-overview

# Dashboard
open http://localhost:8501

# Airflow
open http://localhost:8083   # admin / admin
```

## Paso 5: Desarrollo local
```bash
# Crear virtualenv Python
python3 -m venv .venv
source .venv/bin/activate

# Instalar herramientas de desarrollo
pip install -e ".[dev]"

# Linting
ruff check src/ tests/
ruff format src/ tests/

# Tests
pytest tests/unit/ -v

# dbt compile (verificar SQL sin ejecutar)
cd src/transformation/dbt_cryptolake
dbt compile --profiles-dir . --target ci
```

## Troubleshooting

**Docker: "Not enough memory"**
→ Docker Desktop → Settings → Resources → Asignar al menos 8 GB RAM.

**dbt: "Permission denied" en logs/ o target/**
→ `sudo chown -R $USER:$USER src/transformation/dbt_cryptolake/logs/ src/transformation/dbt_cryptolake/target/`

**Spark Thrift: "Connection refused"**
→ Verificar que el contenedor está corriendo: `docker logs cryptolake-spark-thrift 2>&1 | tail -10`

**Ruff: E402 errors en scripts Spark**
→ Configurado en `ruff.toml` como ignorados (son docstrings antes de imports).
