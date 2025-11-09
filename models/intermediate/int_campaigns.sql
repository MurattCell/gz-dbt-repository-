-- int_campaigns.sql
select *
from {{ref("stg_raw_adwords")}}
UNION ALL
select *
from {{ref("stg_raw_bing")}}
UNION ALL
select *
from {{ref("stg_raw_criteo")}}
UNION ALL
select *
from {{ref("stg_raw_facebook")}}






