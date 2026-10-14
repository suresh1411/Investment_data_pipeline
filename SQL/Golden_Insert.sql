
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
WHEN NOT MATCHED THEN
INSERT (
  Customer_Id, Customer_name, Email, Phone, Address_,
  Join_date, Loaded_timestamp,Team_id,
  Effective_Start_Date, Effective_End_Date, Is_Current
)
VALUES (
  S.Customer_Id, S.Customer_name, S.Email, S.Phone, S.Address_,
  S.Join_date,CURRENT_TIMESTAMP(),Team_id,
  CURRENT_TIMESTAMP(), TIMESTAMP '9999-12-31 23:59:59+00', TRUE
);
