# Architecture Decision Records (ADRs)

Documento que explica las decisiones técnicas del proyecto CryptoLake
y las alternativas que se evaluaron.

---

## ADR-001: Apache Iceberg como Table Format

**Fecha**: 2025-01  
**Estado**: Aceptado

**Contexto**:
Necesitamos un formato de tabla que soporte ACID transactions, schema
evolution, y time travel sobre object storage (S3/MinIO).

**Alternativas evaluadas**:
| Opción | Pros | Contras |
|--------|------|---------|
| **Apache Iceberg** | ACID, time travel, schema evolution, REST catalog, adoptado por AWS/Snowflake/Databricks | Ecosistema más joven que Hive |
| Delta Lake | Buena integración con Spark, open source | Muy ligado a Databricks, menos portable |
| Apache Hudi | Buen soporte de upserts | Más complejo de configurar, comunidad más pequeña |
| Hive tables | Simple, maduro | Sin ACID, sin time travel, sin schema evolution |

**Decisión**: Apache Iceberg  
**Justificación**: Es el formato lakehouse que más tracción tiene en 2025-2026,
con soporte nativo en AWS (Athena, EMR, Glue), Snowflake, y Databricks.
El REST catalog simplifica la configuración en Docker.

---

## ADR-002: dbt para transformaciones Gold

**Fecha**: 2025-01  
**Estado**: Aceptado

**Contexto**:
La capa Gold requiere modelado dimensional (star schema) con lógica
SQL compleja (window functions, joins).

**Alternativas evaluadas**:
| Opción | Pros | Contras |
|--------|------|---------|
| **dbt-core + dbt-spark** | SQL declarativo, tests, docs, estándar industria | Requiere Thrift Server, conflictos de deps |
| PySpark puro | Control total, sin dependencias extra | Mucho boilerplate, sin tests declarativos |
| SQLMesh | Alternativa moderna a dbt | Comunidad pequeña, menos ofertas de trabajo |

**Decisión**: dbt-core con dbt-spark  
**Justificación**: dbt es el estándar de la industria para la "T" de ELT.
Saber dbt es un diferenciador en entrevistas. Los conflictos de dependencias
con Airflow se resuelven con un virtualenv aislado.

---

## ADR-003: Apache Airflow para orquestación

**Fecha**: 2025-01  
**Estado**: Aceptado

**Contexto**:
Necesitamos ejecutar el pipeline completo diariamente con dependencias
entre tareas, reintentos, y monitorización.

**Alternativas evaluadas**:
| Opción | Pros | Contras |
|--------|------|---------|
| **Apache Airflow** | Estándar industria (16% ofertas DE), DAGs Python, UI web | Pesado en recursos, setup complejo |
| Prefect | API moderna, más pythónico | Menos ofertas de trabajo |
| Dagster | Asset-based, buen testing | Comunidad más pequeña |
| Cron + scripts | Simple, sin dependencias | Sin UI, sin reintentos, sin dependencias |

**Decisión**: Apache Airflow 2.9  
**Justificación**: Es la herramienta de orquestación más demandada en ofertas
de trabajo de Data Engineering. La UI web permite monitorizar visualmente
el pipeline.

---

## ADR-004: MinIO como storage local

**Fecha**: 2025-01  
**Estado**: Aceptado

**Contexto**:
Necesitamos object storage S3-compatible para desarrollo local.

**Decisión**: MinIO  
**Justificación**: API 100% compatible con AWS S3. El código funciona
idéntico en local (MinIO) y en producción (AWS S3). Solo cambian las
credenciales y el endpoint.

---

## ADR-005: FastAPI + Streamlit como serving layer

**Fecha**: 2025-02  
**Estado**: Aceptado

**Contexto**:
Los datos Gold necesitan ser accesibles para análisis sin necesidad
de escribir SQL directamente.

**Alternativas evaluadas**:
| Opción | Pros | Contras |
|--------|------|---------|
| **FastAPI + Streamlit** | API auto-documentada, dashboard rápido en Python | Dos servicios separados |
| Flask + Dash | Todo en uno | Flask más básico, Dash más complejo |
| Superset | Dashboard potente | Muy pesado para un proyecto personal |
| Metabase | Fácil de usar, SQL directo | No demuestra habilidades de código |

**Decisión**: FastAPI (API) + Streamlit (dashboard)  
**Justificación**: FastAPI genera documentación Swagger automática. Streamlit
permite prototipar dashboards rápidamente. La separación API/dashboard
demuestra arquitectura de microservicios.

---

## ADR-006: Custom validators vs Great Expectations

**Fecha**: 2025-02  
**Estado**: Aceptado

**Contexto**:
Necesitamos validar la calidad de datos en cada capa del lakehouse.
La guía original especificaba Great Expectations.

**Decisión**: Validators custom ejecutados en Spark  
**Justificación**: Great Expectations tiene conflictos de dependencias
con el stack actual (protobuf vs Airflow, PyArrow vs PySpark). Los
validators custom ejecutan SQL directamente en Spark sobre las tablas
Iceberg, sin dependencias adicionales y con el mismo nivel de validación.
