
MERGE `suresh-dataengineering.Golden_layer.Customers_curated_data` T
USING (
  SELECT *
  FROM `suresh-dataengineering.Silver_layer.customers_stage_data`
  WHERE Customer_Id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY Customer_Id
    ORDER BY Loaded_timestamp DESC
  ) = 1
) S
ON T.Customer_Id = S.Customer_Id
AND T.Is_Current = TRUE
WHEN MATCHED AND (
  T.Customer_name IS DISTINCT FROM S.Customer_name
  OR T.Email IS DISTINCT FROM S.Email
  OR T.Phone IS DISTINCT FROM S.Phone
  OR T.Address_ IS DISTINCT FROM S.Address_
  OR T.Join_date IS DISTINCT FROM S.Join_date
  OR t.Team_id IS DISTINCT FROM S.Team_id
)
THEN UPDATE SET
  T.Is_Current = FALSE,
  T.Effective_End_Date = CURRENT_TIMESTAMP();
