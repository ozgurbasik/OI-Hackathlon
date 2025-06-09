
WITH RECURSIVE product_hierarchy AS (
    -- Base case: top-level categories
    SELECT 
        id,
        name,
        parent_category_id,
        0 as level,
        CAST(name AS VARCHAR(500)) as hierarchy_path
    FROM categories
    WHERE parent_category_id IS NULL
    
    UNION ALL
    
    -- Recursive case: child categories
    SELECT 
        c.id,
        c.name,
        c.parent_category_id,
        ph.level + 1,
        CAST(ph.hierarchy_path + ' > ' + c.name AS VARCHAR(500))
    FROM categories c
    JOIN product_hierarchy ph ON c.parent_category_id = ph.id
    WHERE ph.level < 5
),
inventory_summary AS (
    SELECT 
        p.id as product_id,
        p.name as product_name,
        p.category_id,
        i.quantity_on_hand,
        i.reorder_level,
        i.last_restock_date,
        s.name as supplier_name,
        CASE 
            WHEN i.quantity_on_hand <= i.reorder_level THEN 'LOW_STOCK'
            WHEN i.quantity_on_hand <= (i.reorder_level * 1.5) THEN 'MEDIUM_STOCK'
            ELSE 'HIGH_STOCK'
        END as stock_status
    FROM products p
    JOIN inventory i ON p.id = i.product_id
    LEFT JOIN suppliers s ON p.supplier_id = s.id
    WHERE p.active = 1
),
stock_analysis AS (
    SELECT 
        is_summary.category_id,
        ph.hierarchy_path,
        COUNT(is_summary.product_id) as total_products,
        SUM(is_summary.quantity_on_hand) as total_inventory,
        AVG(is_summary.quantity_on_hand) as avg_inventory,
        SUM(CASE WHEN is_summary.stock_status = 'LOW_STOCK' THEN 1 ELSE 0 END) as low_stock_count,
        SUM(CASE WHEN is_summary.last_restock_date < DATEADD(day, -30, GETDATE()) THEN 1 ELSE 0 END) as stale_inventory_count
    FROM inventory_summary is_summary
    JOIN product_hierarchy ph ON is_summary.category_id = ph.id
    GROUP BY is_summary.category_id, ph.hierarchy_path
)
SELECT 
    sa.category_id,
    sa.hierarchy_path,
    sa.total_products,
    sa.total_inventory,
    sa.avg_inventory,
    sa.low_stock_count,
    sa.stale_inventory_count,
    ROUND((sa.low_stock_count * 100.0 / sa.total_products), 2) as low_stock_percentage,
    ROUND((sa.stale_inventory_count * 100.0 / sa.total_products), 2) as stale_inventory_percentage
FROM stock_analysis sa
ORDER BY sa.low_stock_percentage DESC, sa.total_products DESC;

-- Additional query for reorder recommendations
WITH inventory_summary AS (
    SELECT 
        p.id as product_id,
        p.name as product_name,
        i.quantity_on_hand,
        i.reorder_level,
        s.name as supplier_name
    FROM products p
    JOIN inventory i ON p.id = i.product_id
    LEFT JOIN suppliers s ON p.supplier_id = s.id
    WHERE p.active = 1 AND i.quantity_on_hand <= i.reorder_level
)
SELECT * FROM inventory_summary
ORDER BY quantity_on_hand ASC;