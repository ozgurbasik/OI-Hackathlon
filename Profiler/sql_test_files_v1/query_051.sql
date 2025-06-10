
WITH monthly_sales AS (
    SELECT 
        s.salesperson_id,
        DATE_TRUNC('month', s.sale_date) as sale_month,
        SUM(s.amount) as monthly_total,
        COUNT(s.id) as transaction_count
    FROM sales s
    WHERE s.sale_date >= DATEADD(month, -12, GETDATE())
    GROUP BY s.salesperson_id, DATE_TRUNC('month', s.sale_date)
),
salesperson_rankings AS (
    SELECT 
        ms.salesperson_id,
        ms.sale_month,
        ms.monthly_total,
        ms.transaction_count,
        ROW_NUMBER() OVER (PARTITION BY ms.sale_month ORDER BY ms.monthly_total DESC) as monthly_rank,
        LAG(ms.monthly_total, 1) OVER (PARTITION BY ms.salesperson_id ORDER BY ms.sale_month) as prev_month_sales
    FROM monthly_sales ms
),
performance_metrics AS (
    SELECT 
        sr.salesperson_id,
        sr.sale_month,
        sr.monthly_total,
        sr.transaction_count,
        sr.monthly_rank,
        CASE 
            WHEN sr.prev_month_sales IS NULL THEN 0
            ELSE ((sr.monthly_total - sr.prev_month_sales) / sr.prev_month_sales) * 100
        END as growth_percentage
    FROM salesperson_rankings sr
)
SELECT 
    sp.id,
    sp.first_name,
    sp.last_name,
    pm.sale_month,
    pm.monthly_total,
    pm.transaction_count,
    pm.monthly_rank,
    pm.growth_percentage,
    AVG(pm.monthly_total) OVER (PARTITION BY pm.salesperson_id) as avg_monthly_sales,
    SUM(pm.monthly_total) OVER (PARTITION BY pm.salesperson_id) as total_yearly_sales
FROM salespeople sp
JOIN performance_metrics pm ON sp.id = pm.salesperson_id
WHERE sp.active = 1
ORDER BY pm.sale_month DESC, pm.monthly_rank ASC;

-- Duplicate analysis for additional complexity
WITH monthly_sales AS (
    SELECT 
        s.salesperson_id,
        DATE_TRUNC('month', s.sale_date) as sale_month,
        SUM(s.amount) as monthly_total,
        COUNT(s.id) as transaction_count
    FROM sales s
    WHERE s.sale_date >= DATEADD(month, -12, GETDATE())
    GROUP BY s.salesperson_id, DATE_TRUNC('month', s.sale_date)
),
salesperson_rankings AS (
    SELECT 
        ms.salesperson_id,
        ms.sale_month,
        ms.monthly_total,
        ms.transaction_count,
        ROW_NUMBER() OVER (PARTITION BY ms.sale_month ORDER BY ms.monthly_total DESC) as monthly_rank,
        LAG(ms.monthly_total, 1) OVER (PARTITION BY ms.salesperson_id ORDER BY ms.sale_month) as prev_month_sales
    FROM monthly_sales ms
)
SELECT 
    COUNT(*) as total_records,
    AVG(sr.monthly_total) as avg_sales,
    MAX(sr.monthly_rank) as max_rank
FROM salesperson_rankings sr;