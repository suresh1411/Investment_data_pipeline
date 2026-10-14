
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
# 1. CONFIGURATION
# ==================================================

PROJECT_ID = "suresh-dataengineering"

# Source GCS file
SOURCE_BUCKET = "source_landing_zone"
SOURCE_FILE = "customers.csv"

# BigQuery location: all datasets are in US
BQ_LOCATION = "US"

# BigQuery tables
BRONZE_TABLE = (
    "suresh-dataengineering.Bronze_layer.customers_raw_data"
)

SILVER_TABLE = (
    "suresh-dataengineering.Silver_layer.customers_stage_data"
)

GOLDEN_TABLE = (
    "suresh-dataengineering.Golden_layer.Customers_curated_data"
)

BRONZE_AUDIT_TABLE = (
    "suresh-dataengineering.Audit_layer.bronze_audit_table"
)

SILVER_AUDIT_TABLE = (
    "suresh-dataengineering.Audit_layer.silver_audit_table"
)


# ==================================================
# 2. SQL FILE PATH
# ==================================================

# Expected structure:
# dags/
#   Invest_pipeline_dag.py
#   SQL/
#     Bronze.sql
#     Silver.sql
#     Golden_create.sql
#     Golden_Expire.sql
#     Golden_Insert.sql
#     Audit.sql

DAG_DIR = Path(__file__).resolve().parent
SQL_DIR = DAG_DIR / "SQL"


def read_sql(filename):
    """Read SQL files uploaded beside this DAG."""

    sql_path = SQL_DIR / filename

    if not sql_path.is_file():
        raise FileNotFoundError(
            f"SQL file not found: {sql_path}"
        )

    return sql_path.read_text(encoding="utf-8")


# ==================================================
# 3. DEFAULT ARGUMENTS
# ==================================================

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ==================================================
# 4. DAG DEFINITION
# ==================================================

with DAG(
    dag_id="investment_customer_pipeline",
    description="GCS to BigQuery Bronze, Silver and Golden pipeline",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["gcp", "bigquery", "customer", "etl"],
) as dag:

    # --------------------------------------------------
    # TASK 1: CREATE BRONZE TABLE
    # --------------------------------------------------

    create_bronze_table = BigQueryInsertJobOperator(
        task_id="create_bronze_table",
        configuration={
            "query": {
                "query": read_sql("Bronze.sql"),
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    # --------------------------------------------------
    # TASK 2: CREATE GOLDEN TABLE
    # --------------------------------------------------

    create_golden_table = BigQueryInsertJobOperator(
        task_id="create_golden_table",
        configuration={
            "query": {
                "query": read_sql("Golden_create.sql"),
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    # --------------------------------------------------
    # TASK 3: CREATE AUDIT TABLES
    # --------------------------------------------------

    create_audit_tables = BigQueryInsertJobOperator(
        task_id="create_audit_tables",
        configuration={
            "query": {
                "query": read_sql("Audit.sql"),
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    # --------------------------------------------------
    # TASK 4: LOAD GCS CSV INTO BRONZE
    # --------------------------------------------------

    
    load_bronze = GCSToBigQueryOperator(
        task_id="load_gcs_to_bronze",
        bucket=SOURCE_BUCKET,
        source_objects=[SOURCE_FILE],
        destination_project_dataset_table=BRONZE_TABLE,
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=False,
        schema_fields=[
            {
                "name": "Customer_Id",
                "type": "STRING",
                "mode": "NULLABLE",
            },
            {
                "name": "Customer_name",
                "type": "STRING",
                "mode": "NULLABLE",
            },
            {
                "name": "Email",
                "type": "STRING",
                "mode": "NULLABLE",
            },
            {
                "name": "Phone",
                "type": "STRING",
                "mode": "NULLABLE",
            },
            {
                "name": "Address_",
                "type": "STRING",
                "mode": "NULLABLE",
            },
            {
                "name": "Join_date",
                "type": "STRING",
                "mode": "NULLABLE",
            },
        ],
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_NEVER",
        location="US",
    )


    # --------------------------------------------------
    # TASK 5: BRONZE AUDIT
    # --------------------------------------------------

    audit_bronze = BigQueryInsertJobOperator(
        task_id="audit_bronze",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{BRONZE_AUDIT_TABLE}`
                    (
                        run_timestamp,
                        table_name,
                        row_count,
                        status
                    )
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
        location=BQ_LOCATION,
    )

    # --------------------------------------------------
    # TASK 6: LOAD SILVER
    # --------------------------------------------------

    load_silver = BigQueryInsertJobOperator(
        task_id="load_silver",
        configuration={
            "query": {
                "query": read_sql("Silver.sql"),
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    # --------------------------------------------------
    # TASK 7: SILVER AUDIT
    # --------------------------------------------------

    audit_silver = BigQueryInsertJobOperator(
        task_id="audit_silver",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{SILVER_AUDIT_TABLE}`
                    (
                        run_timestamp,
                        table_name,
                        row_count,
                        status
                    )
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
        location=BQ_LOCATION,
    )

    # --------------------------------------------------
    # TASK 8: EXPIRE CHANGED GOLDEN RECORDS
    # SCD TYPE 2
    # --------------------------------------------------

    expire_golden = BigQueryInsertJobOperator(
        task_id="expire_golden_records",
        configuration={
            "query": {
                "query": read_sql("Golden_Expire.sql"),
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    # --------------------------------------------------
    # TASK 9: INSERT NEW GOLDEN VERSIONS
    # --------------------------------------------------

    insert_golden = BigQueryInsertJobOperator(
        task_id="insert_golden_records",
        configuration={
            "query": {
                "query": read_sql("Golden_Insert.sql"),
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    # ==================================================
    # 5. TASK DEPENDENCIES
    # ==================================================

    # Table creation tasks must finish before the load.
    [
        create_bronze_table,
        create_golden_table,
        create_audit_tables,
    ] >> load_bronze

    # Main ETL flow.
    (
        load_bronze
        >> audit_bronze
        >> load_silver
        >> audit_silver
        >> expire_golden
        >> insert_golden
    )
