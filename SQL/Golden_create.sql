
CREATE TABLE IF NOT EXISTS
  `suresh-dataengineering.Golden_layer.Customers_curated_data`
(
  Customer_Id INT64,
  Customer_name STRING,
  Email STRING,
  Phone STRING,
  Address_ STRING,
  Join_date DATE,
  Loaded_timestamp TIMESTAMP,
  Team_id int64,
  Effective_Start_Date TIMESTAMP,
  Effective_End_Date TIMESTAMP,
  Is_Current BOOL
);
