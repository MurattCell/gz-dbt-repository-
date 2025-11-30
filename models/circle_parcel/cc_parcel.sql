{{ config(
  materialized='view'
)}}
WITH nb_products_parcel AS (
   SELECT
    parcel_id,
    SUM(qty) AS qty,
    COUNT(DISTINCT model_name) AS nb_products
   FROM {{ref('stg_cc_parcel_products')}}
   GROUP BY parcel_id
)

SELECT
   t.parcel_id,
   t.parcel_tracking,
   t.transporter,
   t.priority,
   --TARİH
   PARSE_DATE("%B %e, %Y", t.date_purchase) AS date_purchase,
   PARSE_DATE("%B %e, %Y", t.date_shipping) AS date_shipping,
   PARSE_DATE("%B %e, %Y", t.date_delivery) AS date_delivery,
   PARSE_DATE("%B %e, %Y", t.date_cancelled) AS date_cancelled,
   --AY
   EXTRACT( MONTH FROM PARSE_DATE("%B %e, %Y", t.date_purchase)) AS month_purchase,
   --DURUM
   CASE 
     WHEN t.date_cancelled IS NOT NULL THEN 'İptal Edildi'
     WHEN t.date_shipping IS NULL THEN 'Devam Ediyor'
     WHEN t.date_delivery IS NULL THEN 'Taşınıyor'
     WHEN t.date_delivery IS NOT NULL THEN 'Teslim Edildi'
     ELSE NULL
   END AS status,
-- ZAMAN HESAPLAMASI
DATE_DIFF(PARSE_DATE("%B %e, %Y", t.date_shipping), PARSE_DATE("%B %e, %Y", t.date_purchase),DAY) AS expedition_time,
DATE_DIFF(PARSE_DATE("%B %e, %Y", t.date_delivery), PARSE_DATE("%B %e, %Y", t.date_shipping),DAY) AS transport_time,
DATE_DIFF(PARSE_DATE("%B %e, %Y", t.date_delivery), PARSE_DATE("%B %e, %Y", t.date_purchase),DAY) AS delivery_time,
--GECİKME
IF(t.date_delivery IS NULL,NULL,
IF(DATE_DIFF(PARSE_DATE("%B %e, %Y", t.date_delivery), PARSE_DATE("%B %e, %Y", t.date_purchase), DAY)>5, 1, 0)) AS delay,
p.qty,
p.nb_products
FROM {{ref('stg_cc_parcel')}} AS t
LEFT JOIN nb_products_parcel AS p USING (parcel_id)