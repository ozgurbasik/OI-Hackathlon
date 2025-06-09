
WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;

WITH sales_cte AS (
    SELECT s.store_id, SUM(o.total_amount) AS total_sales
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'
    GROUP BY s.store_id
),
top_stores AS (
    SELECT store_id
    FROM sales_cte
    WHERE total_sales > (SELECT AVG(total_sales) FROM sales_cte)
)
SELECT s.store_id, s.region, t.total_sales, 
       RANK() OVER (PARTITION BY s.region ORDER BY t.total_sales DESC) as sales_rank
FROM stores s
JOIN top_stores t ON s.store_id = t.store_id
JOIN sales_cte t2 ON s.store_id = t2.store_id
WHERE s.region IS NOT NULL;
