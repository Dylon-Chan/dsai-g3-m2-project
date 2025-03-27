/*
    Group orders by customers and calculate the total number of orders, customers, items, revenue, and freight cost per customer state and city.
    This analysis will help us understand the distribution of orders, customers, and revenue across different customer locations.
*/

select
    c.customer_state,
    count(distinct o.order_id) as num_orders,
    count(distinct o.customer_id) as num_customers,
    sum(num_items) as total_items,
    sum(total_order_revenue) as total_revenue,
    sum(total_freight_cost) as total_freight_cost
FROM `dsai-g3-m2-project.brazilian_ecommerce_sales.fact_orders` as o
join `dsai-g3-m2-project.brazilian_ecommerce_sales.dim_customers` as c
    on o.customer_id = c.customer_id
group by c.customer_state;

select
    c.customer_city,
    count(distinct o.order_id) as num_orders,
    count(distinct o.customer_id) as num_customers,
    sum(num_items) as total_items,
    sum(total_order_revenue) as total_revenue,
    sum(total_freight_cost) as total_freight_cost
FROM `dsai-g3-m2-project.brazilian_ecommerce_sales.fact_orders` as o
join `dsai-g3-m2-project.brazilian_ecommerce_sales.dim_customers` as c
    on o.customer_id = c.customer_id
group by c.customer_city;