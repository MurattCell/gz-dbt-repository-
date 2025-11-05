-- models/staging/stg_raw__ship.sql
with source as (
    select * from {{ source('raw', 'ship') }}
),
renamed as (
    select
        orders_id,
        shipping_fee,
        logcost AS log_cost,
        CAST(ship_cost AS INT64) AS ship_cost
    from source
)
select * from renamed