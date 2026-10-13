
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)

# ==================================================
# GCP CONFIGURATION
# ==================================================

PROJECT_ID = "suresh-dataengineering"

BUCKET = "source_landing_zone"
SOURCE_OBJECT = "customers.csv"

BRONZE_TABLE = (
    f"{PROJECT_ID}.Bronze_layer.customers_raw_data"
)
SILVER_TABLE = (
    f"{PROJECT_ID}.Silver_layer.customers_stage_data"
)
GOLDEN_TABLE = (
    f"{PROJECT_ID}.Golden_layer.Customers_curated_data"
)

# Both audit tables use your existing Audit_layer dataset.
BRONZE_AUDIT_TABLE = (
    f"{PROJECT_ID}.Audit_layer.bronze_audit_table"
)
SILVER_AUDIT_TABLE = (
    f"{PROJECT_ID}.Audit_layer.silver_audit_table"
)

# SQL and DAG folders are siblings in your VS Code project.
SQL_DIR = Path(__file__).resolve().parent.parent / "SQL"


def read_sql(filename):
    return (SQL_DIR / filename).read_text(encoding="utf-8")


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="customer_bronze_silver_golden_pipeline",
    default_args=default_args,
    description="Customer pipeline with BigQuery audit and SCD Type 2",
    start_date=datetime(2026, 10, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["customer", "bigquery", "scd2"],
) as dag:

    # 1. Create Bronze table if needed.
    create_bronze = BigQueryInsertJobOperator(
        task_id="create_bronze_table",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": read_sql("Bronze.sql"),
                "useLegacySql": False,
            }
        },
    )

    # 2. Create Golden table if needed.
    create_golden = BigQueryInsertJobOperator(
        task_id="create_golden_table",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": read_sql("Golden_create.sql"),
                "useLegacySql": False,
            }
        },
    )

    # 3. Create audit tables if needed.
    create_audit_tables = BigQueryInsertJobOperator(
        task_id="create_audit_tables",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": read_sql("Audit.sql"),
                "useLegacySql": False,
            }
        },
    )

    # 4. Load customers.csv from GCS into Bronze.
    load_bronze = GCSToBigQueryOperator(
        task_id="load_gcs_to_bronze",
        bucket=BUCKET,
        source_objects=[SOURCE_OBJECT],
        destination_project_dataset_table=BRONZE_TABLE,
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=False,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        project_id=PROJECT_ID,
    )

    # 5. Record Bronze audit information.
    audit_bronze = BigQueryInsertJobOperator(
        task_id="audit_bronze",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{BRONZE_AUDIT_TABLE}`
                    (run_timestamp, table_name, row_count, status)
                    SELECT
                        CURRENT_TIMESTAMP(),
                        '{BRONZE_TABLE}',
                        COUNT(*),
                        'SUCCESS'
                    FROM `{BRONZE_TABLE}`
                """,
                "useLegacySql": False,
            }
        },
    )

    # 6. Transform Bronze into Silver.
    load_silver = BigQueryInsertJobOperator(
        task_id="load_silver",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": read_sql("Silver.sql"),
                "useLegacySql": False,
            }
        },
    )

    # 7. Record Silver audit information.
    audit_silver = BigQueryInsertJobOperator(
        task_id="audit_silver",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{SILVER_AUDIT_TABLE}`
                    (run_timestamp, table_name, row_count, status)
                    SELECT
                        CURRENT_TIMESTAMP(),
                        '{SILVER_TABLE}',
                        COUNT(*),
                        'SUCCESS'
                    FROM `{SILVER_TABLE}`
                """,
                "useLegacySql": False,
            }
        },
    )

    # 8. Expire old Golden versions when details change.
    expire_golden = BigQueryInsertJobOperator(
        task_id="expire_old_customer_versions",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": read_sql("Golden_Expire.sql"),
                "useLegacySql": False,
            }
        },
    )

    # 9. Insert new customers and new SCD Type 2 versions.
    insert_golden = BigQueryInsertJobOperator(
        task_id="insert_new_customer_versions",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": read_sql("Golden_Insert.sql"),
                "useLegacySql": False,
            }
        },
    )

    # Execute setup tasks first, then the pipeline in sequence.
    [create_bronze, create_golden, create_audit_tables] >> load_bronze

    (
        load_bronze
        >> audit_bronze
        >> load_silver
        >> audit_silver
        >> expire_golden
        >> insert_golden
    )
