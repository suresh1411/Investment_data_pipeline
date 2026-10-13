# PCCM Card Data Pipeline

## Project Overview

This project implements a data engineering pipeline using Google Cloud Platform (GCP).

The pipeline processes PCCM card data from the GCS landing zone and loads it into different BigQuery layers.

## Architecture

GCS Landing Zone
        ↓
Bronze Layer
        ↓
Silver Layer
        ↓
Golden Layer

## Technologies Used

- Google Cloud Storage (GCS)
- BigQuery
- Cloud Composer
- Apache Airflow
- SQL
- Git
- GitHub

## Data Pipeline

### 1. GCS Landing Zone

Source files are received in the GCS landing bucket.

Example:

gs://source_landing_zone/

### 2. Bronze Layer

Raw data from GCS is loaded into the BigQuery Bronze layer.

Example table:

Bronze_layer.Pccm_card_raw_data

### 3. Silver Layer

Data is cleaned and transformed.

Typical transformations include:

- Removing duplicate records
- Trimming spaces
- Handling NULL values
- Data type conversion
- Data validation

Example table:

Silver_layer.Pccm_card_stg_data

### 4. Golden Layer

Business-ready and curated data is created from the Silver layer.

Example table:

Golden_layer.Pccm_card_curated_data

## Airflow

Cloud Composer is used to orchestrate the pipeline.

The Airflow DAG performs:

GCS
 ↓
Bronze
 ↓
Silver
 ↓
Golden

## Project Structure

```text
PCCM-Card-Data-Pipeline/
│
├── dags/
│   └── pccm_card_dag.py
│
├── sql/
│   ├── bronze.sql
│   ├── silver.sql
│   └── golden.sql
│
├── tests/
│   └── validation.sql
│
├── docs/
│   └── project_documentation.md
│
├── .gitignore
└── README.md