from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
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
SOURCE_BUCKET = "source_landing_zone"
SOURCE_FILE = "customers2.csv"

# BigQuery datasets are in the US multi-region.
BQ_LOCATION = "US"

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
GOLDEN_AUDIT_TABLE = (
    "suresh-dataengineering.Audit_layer.Golden_audit_table"
)


# ==================================================
# 2. SQL FILE PATHS
# Composer bucket structure:
# dags/
#   Invest_pipeline_dag.py
#   SQL/
#     Bronze.sql
#     Silver.sql
#     Golden_create.sql
#     Golden_Expire.sql
#     Golden_Insert.sql
#     Audit.sql
# ==================================================
DAG_DIR = Path(__file__).resolve().parent
SQL_DIR = DAG_DIR / "SQL"


def read_sql(filename: str) -> str:
    """Read a SQL file from the SQL folder beside this DAG."""
    sql_path = SQL_DIR / filename

    if not sql_path.is_file():
        raise FileNotFoundError(
            f"SQL file not found: {sql_path}"
        )

    return sql_path.read_text(encoding="utf-8")


# ==================================================
# 3. GET ROW COUNT FOR THE CURRENT BRONZE LOAD
# ==================================================
def get_bronze_ingested_count(**context) -> int:
    """
    Get the output_rows value from this run's BigQuery load job.
    This is the number of rows loaded by this job, not the
    accumulated number of rows in the Bronze table.
    """
    ti = context["ti"]

    load_job_id = ti.xcom_pull(
        task_ids="load_gcs_to_bronze",
        key="job_id",
    )

    if not load_job_id:
        raise ValueError(
            "BigQuery load job ID was not found in XCom."
        )

    hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        location=BQ_LOCATION,
        use_legacy_sql=False,
    )

    job = hook.get_job(
        job_id=load_job_id,
        project_id=PROJECT_ID,
        location=BQ_LOCATION,
    )

    if job.output_rows is None:
        raise ValueError(
            f"No output_rows value returned by job {load_job_id}."
        )

    ingested_count = int(job.output_rows)

    ti.log.info(
        "Rows loaded in this Bronze run: %s",
        ingested_count,
    )

    return ingested_count


# ==================================================
# 4. GET ROW COUNT INSERTED INTO GOLDEN THIS RUN
# ==================================================
def get_golden_ingested_count(**context) -> int:
    """
    Get the number of rows affected by the Golden INSERT job.
    This is the current job's inserted-row count, not the total
    number of rows already stored in Golden.
    """
    ti = context["ti"]

    golden_job_id = ti.xcom_pull(
        task_ids="insert_golden_records",
        key="return_value",
    )

    if not golden_job_id:
        raise ValueError(
            "Golden insert job ID was not found in XCom."
        )

    hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        location=BQ_LOCATION,
        use_legacy_sql=False,
    )

    job = hook.get_job(
        job_id=golden_job_id,
        project_id=PROJECT_ID,
        location=BQ_LOCATION,
    )

    affected_rows = job.num_dml_affected_rows

    if affected_rows is None:
        raise ValueError(
            "BigQuery did not return the affected-row count "
            f"for Golden insert job {golden_job_id}."
        )

    ingested_count = int(affected_rows)

    ti.log.info(
        "Rows inserted by this Golden run: %s",
        ingested_count,
    )

    return ingested_count


# ==================================================
# 5. DEFAULT ARGUMENTS
# ==================================================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ==================================================
# 6. DAG DEFINITION
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
    # Audit.sql should create Bronze, Silver and Golden audits.
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
    # TASK 4: LOAD CSV FROM GCS INTO BRONZE
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
        location=BQ_LOCATION,
    )

    # --------------------------------------------------
    # TASK 5: GET ROWS LOADED IN THIS BRONZE RUN
    # --------------------------------------------------
    bronze_ingested_count = PythonOperator(
        task_id="get_bronze_ingested_count",
        python_callable=get_bronze_ingested_count,
    )

    # --------------------------------------------------
    # TASK 6: BRONZE AUDIT
    # row_count = rows loaded in this run
    # Total_records_count = total rows in Bronze after load
    # --------------------------------------------------
    audit_bronze = BigQueryInsertJobOperator(
        task_id="audit_bronze",
        configuration={
            "query": {
                "query": """
                    INSERT INTO
                      `suresh-dataengineering.Audit_layer.bronze_audit_table`
                    (
                      run_timestamp,
                      table_name,
                      row_count,
                      status,
                      Total_records_count
                    )
                    SELECT
                      CURRENT_TIMESTAMP(),
                      'Bronze_layer.customers_raw_data',
                      CAST(
                        {{ ti.xcom_pull(
                            task_ids='get_bronze_ingested_count'
                        ) | int }}
                        AS INT64
                      ),
                      'SUCCESS',
                      (
                        SELECT COUNT(*)
                        FROM
                          `suresh-dataengineering.Bronze_layer.customers_raw_data`
                      )
                """,
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    # --------------------------------------------------
    # TASK 7: LOAD SILVER
    # Assumes Silver.sql rebuilds the complete Silver table.
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
    # TASK 8: SILVER AUDIT
    # The resulting Silver count is recorded in both columns.
    # --------------------------------------------------
    audit_silver = BigQueryInsertJobOperator(
        task_id="audit_silver",
        configuration={
            "query": {
                "query": """
                    INSERT INTO
                      `suresh-dataengineering.Audit_layer.silver_audit_table`
                    (
                      run_timestamp,
                      table_name,
                      row_count,
                      status,
                      Total_records_count
                    )
                    SELECT
                      CURRENT_TIMESTAMP(),
                      'Silver_layer.customers_stage_data',
                      COUNT(*),
                      'SUCCESS',
                      COUNT(*)
                    FROM
                      `suresh-dataengineering.Silver_layer.customers_stage_data`
                """,
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    # --------------------------------------------------
    # TASK 9: EXPIRE CHANGED GOLDEN RECORDS (SCD TYPE 2)
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
    # TASK 10: INSERT NEW GOLDEN VERSIONS
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

    # --------------------------------------------------
    # TASK 11: GET ROWS INSERTED INTO GOLDEN THIS RUN
    # --------------------------------------------------
    golden_ingested_count = PythonOperator(
        task_id="get_golden_ingested_count",
        python_callable=get_golden_ingested_count,
    )

    # --------------------------------------------------
    # TASK 12: GOLDEN AUDIT
    # ingest_row_count = rows inserted by the current INSERT job
    # Total_records_count = all rows in Golden after processing
    # --------------------------------------------------
    audit_golden = BigQueryInsertJobOperator(
        task_id="audit_golden",
        configuration={
            "query": {
                "query": """
                    INSERT INTO
                      `suresh-dataengineering.Audit_layer.Golden_audit_table`
                    (
                      run_timestamp,
                      table_name,
                      ingest_row_count,
                      Status,
                      Total_records_count
                    )
                    SELECT
                      CURRENT_TIMESTAMP(),
                      'Golden_layer.Customers_curated_data',
                      CAST(
                        {{ ti.xcom_pull(
                            task_ids='get_golden_ingested_count'
                        ) | int }}
                        AS INT64
                      ),
                      'SUCCESS',
                      (
                        SELECT COUNT(*)
                        FROM
                          `suresh-dataengineering.Golden_layer.Customers_curated_data`
                      )
                """,
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    # ==================================================
    # 7. TASK DEPENDENCIES
    # ==================================================

    # Setup tasks run before the Bronze load.
    [
        create_bronze_table,
        create_golden_table,
        create_audit_tables,
    ] >> load_bronze

    # Bronze: load -> count -> audit.
    load_bronze >> bronze_ingested_count >> audit_bronze

    # Silver -> Golden SCD Type 2 -> Golden audit.
    (
        audit_bronze
        >> load_silver
        >> audit_silver
        >> expire_golden
        >> insert_golden
        >> golden_ingested_count
        >> audit_golden
    )