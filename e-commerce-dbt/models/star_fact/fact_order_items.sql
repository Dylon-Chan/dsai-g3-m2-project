
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/
{{ config(materialized='table') }}

with orders as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp
    from {{ source('gcs_ingestion', 'olist_orders_dataset') }} as o
),

order_items as (
    select
        oi.order_id,
        order_item_id,
        product_id,   
        price 
    from {{ source('gcs_ingestion', 'olist_order_items_dataset') }} as oi
)

select
    od.order_id,
    od.customer_id,
    od.order_status,
    od.order_purchase_timestamp,
    oi.order_item_id,
    oi.product_id,
    oi.price
from orders od join order_items oi
    on od.order_id = oi.order_id