# End-to-End Ecommerce Data Pipeline

## Overview

This project demonstrates a modern Data Engineering solution that builds an automated, scalable, and production-style ELT pipeline using the Modern Data Stack.

The pipeline ingests customer data from PostgreSQL using Airbyte, stores raw and processed data in a Databricks Lakehouse powered by Delta Lake, transforms data using dbt Core, and orchestrates the entire workflow through Apache Airflow and Astronomer Cosmos.

The final output consists of analytics-ready Gold Layer data models that can be consumed by BI and reporting tools.

---

## Project Architecture

### Data Flow

PostgreSQL → Airbyte → Databricks Delta Lake → dbt Core → Gold Analytics Models

### Technology Stack

| Layer                   | Technology                  |
| ----------------------- | --------------------------- |
| Source Database         | PostgreSQL                  |
| Data Ingestion          | Airbyte                     |
| Storage                 | Databricks Lakehouse        |
| Table Format            | Delta Lake                  |
| Transformation          | dbt Core                    |
| Orchestration           | Apache Airflow              |
| dbt-Airflow Integration | Astronomer Cosmos           |
| Analytics               | Power BI / Tableau / Looker |

---

## Project Objectives

* Build an end-to-end ELT pipeline
* Implement incremental data ingestion
* Create a Lakehouse architecture using Delta Lake
* Apply data transformations with dbt Core
* Automate workflows using Apache Airflow
* Generate analytics-ready business models
* Demonstrate production-style Data Engineering practices

---

## Architecture Layers

## Dataset

Download the **Brazilian E-Commerce Public Dataset by Olist** from Kaggle and unzip the CSVs into `data/olist/`:

[https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

Then load into the source PostgreSQL database:

```bash
uv run python airflow-docker/scripts/load_olist_to_postgres.py --data-dir ./data/olist
```

---

### 1. Source Layer

**PostgreSQL**

**Olist Brazilian E-Commerce Public Dataset** (Kaggle) — 9 tables of real e-commerce
transactions, loaded into PostgreSQL via `airflow-docker/scripts/load_olist_to_postgres.py`.

---

### 2. Ingestion Layer

**Airbyte**

Features:

* Incremental Data Loading
* Change Data Capture (CDC)
* Schema Mapping
* Connection Monitoring

Data is extracted from PostgreSQL and loaded into Databricks Delta Lake.

---

### 3. Lakehouse Layer

**Databricks + Delta Lake**

#### Raw Layer

Stores ingested source data with Airbyte metadata.

Example:

```sql
customers
```

#### Staging Layer

Cleaned and standardized datasets.

Example:

```sql
stg_customer
```

#### Gold Layer

Business-ready analytical models.

Models:

* customer_kpi
* customer_category
* revenue_by_location
* top_customers

---

### 4. Transformation Layer

**dbt Core**

Transformation workflow:

```text
Raw Layer
    ↓
Staging Models
    ↓
Gold Models
```

Features:

* Modular SQL transformations
* Incremental processing
* Data lineage
* Testing and documentation

---

### 5. Orchestration Layer

**Apache Airflow + Astronomer Cosmos**

Pipeline execution sequence:

```text
Trigger Airbyte Sync
        ↓
Wait for Airbyte Completion
        ↓
Run dbt Models
        ↓
Pipeline Complete
```

Airflow DAG automates the complete workflow from ingestion to transformation.

---

## Incremental Processing

### Airbyte Incremental Sync

Validation Process:

1. Initial full load completed
2. New records inserted into PostgreSQL
3. Airbyte detected only new records
4. Incremental synchronization executed successfully

Benefits:

* Reduced processing time
* Lower compute costs
* Faster pipeline execution

---

### dbt Incremental Models

Configuration:

```yaml
materialized: incremental
incremental_strategy: merge
unique_key: customer_id
```

Benefits:

* Processes only new or updated records
* Efficient table maintenance
* Scalable transformation workloads

---

## Gold Layer Data Products

### customer_kpi

Customer performance metrics and KPIs.

### customer_category

Customer segmentation and categorization.

### revenue_by_location

Revenue analysis by geographic location.

### top_customers

High-value customer identification.

---

## Project Structure

```text
project/
│
├── airflow/
│   ├── dags/
│   └── plugins/
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   └── gold/
│   │
│   ├── macros/
│   ├── tests/
│   └── snapshots/
│
├── architecture/
│   └── architecture_diagram.png
│
└── README.md
```

---

## Key Achievements

* End-to-End ELT Pipeline
* Databricks Lakehouse Architecture
* Delta Lake Storage Implementation
* Incremental Data Processing
* Automated Airflow Orchestration
* dbt Gold Layer Transformations
* Analytics-Ready Data Products
* Production-Oriented Data Engineering Workflow

---

## Business Value

This solution demonstrates how organizations can build a modern, scalable, and maintainable analytics platform using open-source and cloud-native technologies.

The architecture supports:

* Faster analytics delivery
* Reliable data pipelines
* Automated workflows
* Scalable data transformations
* Business-ready reporting datasets

---

## Author

**MD Yeasir Arafat Shohan**
Associate Data Engineer
Data Services & Solutions
BRAC IT Services Limited

---

## License

This project is created for learning, portfolio, and demonstration purposes.