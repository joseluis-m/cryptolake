# Architecture Decision Records (ADRs)

Technical decisions made in the CryptoLake project and the alternatives evaluated.

---

## ADR-001: Apache Iceberg as Table Format

**Date**: 2026-02
**Status**: Accepted

**Context**:
The project requires a table format supporting ACID transactions, schema
evolution, and time travel over object storage (S3/MinIO).

**Alternatives evaluated**:
| Option | Pros | Cons |
|--------|------|------|
| **Apache Iceberg** | ACID, time travel, schema evolution, REST catalog, adopted by AWS/Snowflake/Databricks | Younger ecosystem than Hive |
| Delta Lake | Good Spark integration, open source | Tightly coupled to Databricks, less portable |
| Apache Hudi | Strong upsert support | More complex to configure, smaller community |
| Hive tables | Simple, mature | No ACID, no time travel, no schema evolution |

**Decision**: Apache Iceberg
**Rationale**: Iceberg is the lakehouse table format with the most traction
in 2025-2026, with native support in AWS (Athena, EMR, Glue), Snowflake,
and Databricks. The REST catalog simplifies configuration in Docker.

---

## ADR-002: dbt for Gold Transformations

**Date**: 2026-02
**Status**: Accepted

**Context**:
The Gold layer requires dimensional modeling (star schema) with complex
SQL logic (window functions, joins).

**Alternatives evaluated**:
| Option | Pros | Cons |
|--------|------|------|
| **dbt-core + dbt-spark** | Declarative SQL, tests, docs, industry standard | Requires Thrift Server, dependency conflicts |
| Pure PySpark | Full control, no extra dependencies | Verbose boilerplate, no declarative tests |
| SQLMesh | Modern dbt alternative | Small community, lower job market demand |

**Decision**: dbt-core with dbt-spark
**Rationale**: dbt is the industry standard for the "T" in ELT. Proficiency
in dbt is a key differentiator in the job market. Dependency conflicts with
Airflow are resolved using an isolated virtualenv.

---

## ADR-003: Apache Airflow for Orchestration

**Date**: 2026-02
**Status**: Accepted

**Context**:
The pipeline must run daily with task dependencies, retries, and monitoring.

**Alternatives evaluated**:
| Option | Pros | Cons |
|--------|------|------|
| **Apache Airflow** | Industry standard, Python DAGs, web UI | Resource-heavy, complex setup |
| Prefect | Modern API, more Pythonic | Lower job market demand |
| Dagster | Asset-based, good testing | Smaller community |
| Cron + scripts | Simple, no dependencies | No UI, no retries, no dependency management |

**Decision**: Apache Airflow 2.9
**Rationale**: Airflow is the most in-demand orchestration tool in Data
Engineering job postings. The web UI enables visual pipeline monitoring.

---

## ADR-004: MinIO as Local Storage

**Date**: 2026-02
**Status**: Accepted

**Context**:
The project needs S3-compatible object storage for local development.

**Decision**: MinIO
**Rationale**: 100% compatible with the AWS S3 API. Code runs identically
on local (MinIO) and production (AWS S3) environments -- only credentials
and the endpoint change.

---

## ADR-005: FastAPI + Streamlit as Serving Layer

**Date**: 2026-02
**Status**: Accepted

**Context**:
Gold layer data must be accessible for analysis without writing SQL directly.

**Alternatives evaluated**:
| Option | Pros | Cons |
|--------|------|------|
| **FastAPI + Streamlit** | Auto-documented API, rapid Python dashboard | Two separate services |
| Flask + Dash | All-in-one | Flask more basic, Dash more complex |
| Superset | Powerful dashboards | Too heavy for a personal project |
| Metabase | Easy to use, direct SQL | Does not demonstrate coding skills |

**Decision**: FastAPI (API) + Streamlit (dashboard)
**Rationale**: FastAPI generates automatic Swagger documentation. Streamlit
enables rapid dashboard prototyping. The API/dashboard separation
demonstrates a microservices architecture.

---

## ADR-006: Custom Validators vs Great Expectations

**Date**: 2026-02
**Status**: Accepted

**Context**:
Data quality validation is needed at each lakehouse layer.

**Decision**: Custom validators executed in Spark
**Rationale**: Great Expectations has dependency conflicts with the current
stack (protobuf vs Airflow, PyArrow vs PySpark). Custom validators execute
SQL directly in Spark on Iceberg tables, with no additional dependencies
and equivalent validation coverage.
