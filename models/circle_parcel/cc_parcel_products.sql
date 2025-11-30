{{ config(
    materialized='table',
    partition_by={
    "field": "date_purchase",
    "data_type": "date",
    "granularity": "day" 
    }
)}}
SELECT  
    products.parcel_id,
    products.model_name,
    products.qty,
    parcel.date_purchase,
    parcel.status,
    parcel.priority
FROM {{ref('stg_cc_parcel_products')}} AS products
LEFT JOIN {{ref('cc_parcel')}} AS parcel USING(parcel_id)