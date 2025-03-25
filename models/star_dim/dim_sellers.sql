{{ config(materialized='table') }}


with seller_loc as (
    select
        s.seller_id,
        s.seller_zip_code,
        s.seller_city,
        s.seller_state
    from {{ source('gcs_ingestion', 'olist_seller_dataset') }} as s
),


seller_revenue_agg as (
    select
        sr.order_id,
        sr.seller_id
        sum(sr.price) as total_order_revenue,
    from {{ source('gcs_ingestion', 'olist_order_items_dataset') }} as sr
    group by sr.seller_id
)

select
    sl.seller_id,
    sl.seller_zip_code,
    sl.seller_city,
    sl.seller_state,
    sra.order_id,
    sra.total_order_revenue,
from seller_loc sl join seller_revenue_agg sra
    on sl.seller_id = sr.seller_id