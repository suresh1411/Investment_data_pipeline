
CREATE OR REPLACE TABLE
  `suresh-dataengineering.Silver_layer.customers_stage_data`
AS
SELECT
  Customer_Id,
  Customer_name,
  Email,
  Phone,
  Address_,
  COALESCE(
    SAFE.PARSE_DATE('%Y-%m-%d', Join_date),
    SAFE.PARSE_DATE('%d-%m-%Y', Join_date),
    SAFE.PARSE_DATE('%m/%d/%Y', Join_date),
    SAFE.PARSE_DATE('%d/%m/%Y', Join_date)
  ) AS Join_date,
  CURRENT_TIMESTAMP() AS Loaded_timestamp,
  SAFE_CAST(Team_Id AS INT64) AS Team_Id
FROM
  `suresh-dataengineering.Bronze_layer.customers_raw_data`
WHERE
  SAFE_CAST(Customer_Id AS INT64) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY SAFE_CAST(Customer_Id AS INT64)
  ORDER BY Loaded_timestamp DESC
) = 1;
