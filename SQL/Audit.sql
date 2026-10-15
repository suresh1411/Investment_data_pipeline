CREATE TABLE IF NOT EXISTS
  `suresh-dataengineering.Audit_layer.bronze_audit_table`
(
  run_timestamp TIMESTAMP,
  table_name STRING,
  row_count INT64,
  status STRING,
  Total_records_count int64,
);

CREATE TABLE IF NOT EXISTS
  `suresh-dataengineering.Audit_layer.silver_audit_table`
(
  run_timestamp TIMESTAMP,
  table_name STRING,
  row_count INT64,
  status STRING,
  Total_records_count int64
);

CREATE TABLE IF NOT EXISTS 
   `suresh-dataengineering.Audit_layer.Golden_audit_table`
   (
   run_timestamp TIMESTAMP,
   table_name STRING,
   ingest_row_count INT64,
   Status STRING,
   Total_records_count int64
   );
