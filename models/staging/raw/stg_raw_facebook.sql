with

source as (
  select * from {{ source('raw', 'facebook') }} 
),

renamed as (
  select
    date_date,
    paid_source,
    campaign_key,
    campgn_name as campaign_name,
    CAST(ads_cost as float64) as ads_cost,
    impression,
    click
  from source
)

select * from renamed