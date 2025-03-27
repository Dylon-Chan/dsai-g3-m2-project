WITH seller_loc AS (
    SELECT
        s.seller_id,
        s.seller_zip_code_prefix,
        s.seller_city,
        s.seller_state
    FROM {{ source('gcs_ingestion', 'olist_sellers_dataset') }} AS s
),


seller_revenue_agg AS (
    SELECT
        sr.seller_id,
        SUM(sr.price) AS seller_total_order_revenue,
    FROM {{ source('gcs_ingestion', 'olist_order_items_dataset') }} AS sr
    GROUP BY sr.seller_id
)

SELECT
    seller_loc.seller_id,
    seller_loc.seller_zip_code_prefix,
    seller_loc.seller_city,
    seller_loc.seller_state,
    seller_revenue_agg.seller_total_order_revenue,
FROM seller_loc JOIN seller_revenue_agg
    ON seller_loc.seller_id = seller_revenue_agg.seller_id
