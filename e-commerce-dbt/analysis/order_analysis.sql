-- monthly revenue analysis
-- Question: How is our monthly revenue evolving, and what is the month-over-month growth rate?

WITH monthly_revenue AS (
    SELECT 
        DATE_TRUNC(DATE(order_purchase_timestamp), MONTH) AS month,
        ROUND(SUM(total_order_revenue), 2) AS total_monthly_revenue,
        ROUND(AVG(total_order_revenue), 2) AS average_order_revenue,
        COUNT(DISTINCT order_id) AS total_orders
    FROM `dsai-g3-m2-project.brazilian_ecommerce_sales.fact_orders`
    GROUP BY month
)
SELECT 
    curr.month,
    curr.total_monthly_revenue,
    curr.average_order_revenue,
    curr.total_orders,
    ROUND(curr.total_monthly_revenue - COALESCE(prev.total_monthly_revenue, 0), 2) AS revenue_change,
    ROUND(
        SAFE_DIVIDE(curr.total_monthly_revenue - prev.total_monthly_revenue, prev.total_monthly_revenue) * 100, 
        2
    ) AS revenue_growth_percentage
FROM monthly_revenue curr
LEFT JOIN monthly_revenue prev 
ON curr.month = DATE_ADD(prev.month, INTERVAL 1 MONTH) -- Fixed the date arithmetic
ORDER BY curr.month;

-- Order Completion Rate Analysis
-- Question: What is the breakdown of our order statuses, and how does each status contribute to our overall revenue?
SELECT 
    order_status,
    COUNT(*) AS total_orders,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `dsai-g3-m2-project.brazilian_ecommerce_sales.fact_orders`), 2) AS percentage_of_total_orders,
    ROUND(SUM(total_order_revenue), 2) AS total_revenue_by_status,
    ROUND(AVG(total_order_revenue), 2) AS average_revenue_per_order
FROM `dsai-g3-m2-project.brazilian_ecommerce_sales.fact_orders`
GROUP BY order_status
ORDER BY total_orders DESC;


-- Revenue Performance by Delivery Efficiency
-- Question: How does our delivery efficiency correlate with revenue generation, 
-- and what are the revenue implications of different delivery time ranges?

-- SELECT 
--     CASE 
--         WHEN delivery_time_minutes <= 60 THEN '0-1 hour'
--         WHEN delivery_time_minutes <= 360 THEN '1-6 hours'
--         WHEN delivery_time_minutes <= 1440 THEN '6-24 hours'
--         ELSE 'Over 24 hours'
--     END AS delivery_time_bucket,
--     COUNT(*) AS total_orders,
--     ROUND(AVG(total_order_revenue), 2) AS average_order_revenue,
--     ROUND(SUM(total_order_revenue), 2) AS total_revenue,
--     ROUND(SUM(total_freight_cost), 2) AS total_freight_cost,
--     ROUND(SUM(total_order_revenue) - SUM(total_freight_cost), 2) AS net_revenue
-- FROM `dsai-g3-m2-project.brazilian_ecommerce_sales.fact_orders`
-- WHERE order_status = 'delivered'
-- GROUP BY delivery_time_bucket
-- ORDER BY total_revenue DESC;