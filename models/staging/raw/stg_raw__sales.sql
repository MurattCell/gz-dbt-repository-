with source as (
    select * from {{ source('raw', 'sales') }}
)
select
    date_date,
    orders_id,
    pdt_id AS products_id, 
    CAST(revenue AS FLOAT64) AS revenue, 
    CAST(quantity AS INT64) AS quantity 
from source