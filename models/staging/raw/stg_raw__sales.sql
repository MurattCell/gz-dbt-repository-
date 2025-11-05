with source as (
    select * from {{ source('raw', 'sales') }}
)
select
    date_date,
    orders_id,
    pdt_id AS products_id, -- Düzeltme 1: pdt_id -> products_id
    CAST(revenue AS FLOAT64) AS revenue, -- Bölüm 4 için hazırlık
    CAST(quantity AS INT64) AS quantity -- Bölüm 4 için hazırlık
from source