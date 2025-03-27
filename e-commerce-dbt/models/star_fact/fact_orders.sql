
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/
{{ config(materialized='table') }}


with order_details as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date
    from {{ source('gcs_ingestion', 'olist_orders_dataset') }} as o
),

--# ARRAY_AGG(product_id) AS product_ids or GROUP_CONCAT(product_id) AS product_ids to add product and seller info
order_items_agg as (
    select
        oi.order_id,
        count(*) as num_items,
        sum(oi.price) as total_order_revenue,
        sum(oi.freight_value) as total_freight_cost,
        avg(oi.price) as avg_item_price_per_order
    from {{ source('gcs_ingestion', 'olist_order_items_dataset') }} as oi
    group by oi.order_id
),

order_by_customers_state as (

    select
        c.customer_state,
        count(distinct o.order_id) as num_orders,
        count(distinct o.customer_id) as num_customers,
        sum(oia.num_items) as total_items,
        sum(oia.total_order_revenue) as total_revenue,
        sum(oia.total_freight_cost) as total_freight_cost
    from order_details o join order_items_agg oia
        on o.order_id = oia.order_id
    join {{ source('gcs_ingestion', 'olist_customers_dataset') }} as c
        on o.customer_id = c.customer_id
    group by c.customer_state
),

order_by_customers_city as (

    select
        c.customer_city,
        count(distinct o.order_id) as num_orders,
        count(distinct o.customer_id) as num_customers,
        sum(oia.num_items) as total_items,
        sum(oia.total_order_revenue) as total_revenue,
        sum(oia.total_freight_cost) as total_freight_cost
    from order_details o join order_items_agg oia
        on o.order_id = oia.order_id
    join {{ source('gcs_ingestion', 'olist_customers_dataset') }} as c
        on o.customer_id = c.customer_id
    group by c.customer_city
)

select
    od.order_id,
    od.customer_id,
    od.order_status,
    od.order_purchase_timestamp,
    od.order_delivered_customer_date,
    od.order_estimated_delivery_date,
    oia.num_items,
    oia.total_order_revenue,
    oia.total_freight_cost,
    oia.avg_item_price_per_order,
    -- Calculate delivery performance: total time from purchase to customer delivery
    case 
        when od.order_delivered_customer_date is not null then 
            timestamp_diff(od.order_delivered_customer_date, od.order_purchase_timestamp, minute)
        else null
    end as delivery_time_minutes
from order_details od join order_items_agg oia
    on od.order_id = oia.order_id