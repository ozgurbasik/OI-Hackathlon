
WITH quarterly_revenue AS (
    SELECT 
        YEAR(order_date) as year,
        DATEPART(quarter, order_date) as quarter,
        SUM(total_amount) as revenue,
        COUNT(id) as order_count,
        AVG(total_amount) as avg_order_value
    FROM orders
    WHERE order_date >= DATEADD(year, -2, GETDATE())
        AND status = 'COMPLETED'
    GROUP BY YEAR(order_date), DATEPART(quarter, order_date)
),
revenue_trends AS (
    SELECT 
        qr.year,
        qr.quarter,
        qr.revenue,
        qr.order_count,
        qr.avg_order_value,
        LAG(qr.revenue, 1) OVER (ORDER BY qr.year, qr.quarter) as prev_quarter_revenue,
        SUM(qr.revenue) OVER (ORDER BY qr.year, qr.quarter ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) as rolling_4q_revenue
    FROM quarterly_revenue qr
),
customer_segments AS (
    SELECT 
        c.id,
        c.name,
        SUM(o.total_amount) as lifetime_value,
        COUNT(o.id) as total_orders,
        MAX(o.order_date) as last_order_date,
        CASE 
            WHEN SUM(o.total_amount) >= 10000 THEN 'VIP'
            WHEN SUM(o.total_amount) >= 5000 THEN 'PREMIUM'
            WHEN SUM(o.total_amount) >= 1000 THEN 'REGULAR'
            ELSE 'BASIC'
        END as customer_tier
    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id AND o.status = 'COMPLETED'
    WHERE c.active = 1
    GROUP BY c.id, c.name
)
SELECT 
    cs.customer_tier,
    COUNT(cs.id) as customer_count,
    SUM(cs.lifetime_value) as total_segment_value,
    AVG(cs.lifetime_value) as avg_customer_value,
    AVG(cs.total_orders) as avg_orders_per_customer
FROM customer_segments cs
GROUP BY cs.customer_tier
ORDER BY cs.customer_tier;

-- Revenue trend analysis
WITH revenue_trends AS (
    SELECT 
        YEAR(order_date) as year,
        DATEPART(quarter, order_date) as quarter,
        SUM(total_amount) as revenue
    FROM orders
    WHERE order_date >= DATEADD(year, -2, GETDATE())
        AND status = 'COMPLETED'
    GROUP BY YEAR(order_date), DATEPART(quarter, order_date)
)
SELECT 
    rt.year,
    rt.quarter,
    rt.revenue,
    LAG(rt.revenue, 1) OVER (ORDER BY rt.year, rt.quarter) as prev_quarter_revenue
FROM revenue_trends rt
ORDER BY rt.year DESC, rt.quarter DESC;