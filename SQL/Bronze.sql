
CREATE TABLE IF NOT EXISTS
  `suresh-dataengineering.Bronze_layer.customers_raw_data`
(
  Customer_Id STRING,
  Customer_name STRING,
  Email STRING,
  Phone STRING,
  Address_ STRING,
  Join_date STRING
);
 
CREATE TABLE Products IF NOT EXISTS 
(
  Product_id STRING,
  Product_name STRING,
  Category STRING,
  Price STRING,
  TIMESTAMP AS Loaded_timestamp
);

