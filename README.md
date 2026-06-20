# Olist E-Commerce Data Platform

> An end-to-end real-time data engineering pipeline built for the Olist e-commerce platform, processing transaction and customer events from source through a Medallion architecture (Bronze → Silver → Gold) to analytics dashboards.

---

## Table of Contents

- [Overview](#overview)
- [Architecture Diagram](#architecture-diagram)
- [Technology Stack](#technology-stack)
- [Data Pipeline](#data-pipeline)
- [Getting Started](#getting-started)
- [DAGs & Orchestration](#dags--orchestration)

---

## Overview

Olist E-Commerce Data Platform simulates a production-grade data engineering system for an e-commerce platform. Real-time events emitted by the mock producer are ingested through **Apache Kafka**, processed by **Apache Spark** using structured streaming and batch jobs, stored as **Apache Iceberg** tables on **MinIO** (S3-compatible), transformed via **dbt** over **Trino**, and visualized in **Grafana** — all orchestrated by **Apache Airflow**.

---

## Architecture Diagram

<img width="1831" height="737" alt="architecture" src="https://github.com/user-attachments/assets/77cec943-89a3-448d-8b28-0667a2f9b423" />


---

## Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Source** | Mock Producer | Python script simulating real-time e-commerce events (~50 eps) |
| **Ingestion** | Apache Kafka (KRaft 7.5.0) | Real-time event streaming via `olist-events` topic |
| **Processing** | Apache Spark 3.5.1 | Streaming ingestion + batch transformation |
| **Storage** | MinIO (S3-compatible) | Data lake object storage |
| **Table Format** | Apache Iceberg 1.4.2 | ACID transactions, schema evolution, time travel |
| **Catalog** | Project Nessie 0.104.5 | Git-like catalog for Iceberg tables |
| **Transformation** | dbt (Trino adapter) | Build Silver and Gold models, data quality tests |
| **Query Engine** | Trino | SQL queries over Iceberg on MinIO |
| **Orchestration** | Apache Airflow 2.x | DAG scheduling and task dependency management |
| **Serving** | PostgreSQL 15 | Dashboard-ready Gold tables via JDBC export |
| **Visualization** | Grafana | Real-time dashboards connected to PostgreSQL + Trino |
| **Infrastructure** | Docker Compose | Single-command local deployment with Named Volumes |

---
## Data Set
[Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
---

## Data Pipeline

The pipeline runs scheduled (Airflow DAG) in the following order:

```
start
  └─→ check_kafka_health          [Check connection to Kafka broker]
        └─→ verify_iceberg_bronze [Ensure bronze tables are ready]
              └─→ silver_quality_gate [dbt build silver models + data tests]
                    └─→ gold_build     [dbt run gold models]
                          └─→ export_gold_to_postgres  [Spark JDBC → PostgreSQL]
                                └─→ end
```

---

## Getting Started

### Prerequisites

### 1. Clone the repository

```bash
git clone https://github.com/tai20605/Olist_Platform_DE
cd Olist_Platform_DE
```

### 2. Start all services
```bash
docker compose up -d
```

```bash
docker compose ps
```

### 3. Check simulation status (one-time)

The e-commerce event simulation runs automatically as a container (`mock-producer`). You can monitor the mock events being produced to the Kafka topic by visiting the Kafka UI at `http://localhost:8086`.

### 4. Run the main pipeline

Open the Airflow UI at `http://localhost:8085` (airflow / airflow) and trigger the main DAG to process the data:

```
DAG: olist_pipeline → Trigger DAG ▶
```

This runs the full Bronze → Silver → Gold → PostgreSQL pipeline.

### 5. View dashboards

Open Grafana at `http://localhost:3000` to see monitoring dashboards.
<img width="1852" height="1077" alt="dashboard" src="https://github.com/user-attachments/assets/3fa23c41-f2f2-487c-9590-17e1ed56a0ee" />

---

## DAGs & Orchestration

### `olist_pipeline`
Main production DAG. Orchestrates the full Spark + dbt pipeline from Kafka consumption verification through PostgreSQL export.

### `olist_monitoring`
Orchestrates data quality checks, data freshness monitoring and volume snapshots anomaly detection.

