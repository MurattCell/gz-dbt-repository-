SELECT
   ParCEL_id as parcel_id,
   Model_mAME as model_name,
   QUANTITY as qty
FROM {{ source('raw_data_circle','raw_cc_parcel_product') }}