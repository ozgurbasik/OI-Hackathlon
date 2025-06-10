WITH CustomerOrders AS (
    SELECT 
        c.CustomerID,
        c.FirstName,
        c.LastName,
        c.Email,
        o.OrderID,
        o.OrderDate,
        od.ProductID,
        od.Quantity,
        od.UnitPrice,
        p.ProductName,
        p.CategoryID,
        cat.CategoryName,
        (od.Quantity * od.UnitPrice) AS TotalAmount
    FROM Customers c
    INNER JOIN Orders o ON c.CustomerID = o.CustomerID
    INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
    INNER JOIN Products p ON od.ProductID = p.ProductID
    INNER JOIN Categories cat ON p.CategoryID = cat.CategoryID
),
CustomerStats AS (
    SELECT 
        CustomerID,
        SUM(TotalAmount) AS TotalSpent,
        COUNT(DISTINCT OrderID) AS TotalOrders,
        COUNT(DISTINCT CategoryID) AS DistinctCategories,
        CASE 
            WHEN SUM(TotalAmount) >= 5000 THEN 'Platinum'
            WHEN SUM(TotalAmount) >= 2000 THEN 'Gold'
            WHEN SUM(TotalAmount) >= 1000 THEN 'Silver'
            ELSE 'Bronze'
        END AS LoyaltyTier
    FROM CustomerOrders
    GROUP BY CustomerID
),
CategoryAverages AS (
    SELECT 
        CategoryID,
        CategoryName,
        AVG(TotalAmount) AS AvgSpendPerOrder,
        SUM(TotalAmount) AS TotalCategoryRevenue
    FROM CustomerOrders
    GROUP BY CategoryID, CategoryName
),
CustomerCategoryStats AS (
    SELECT 
        co.CustomerID,
        co.CategoryID,
        co.CategoryName,
        SUM(co.TotalAmount) AS CustomerCategorySpend,
        COUNT(DISTINCT co.OrderID) AS OrdersInCategory
    FROM CustomerOrders co
    GROUP BY co.CustomerID, co.CategoryID, co.CategoryName
)
SELECT 
    c.FirstName,
    c.LastName,
    c.Email,
    cs.LoyaltyTier,
    cs.TotalSpent,
    cs.TotalOrders,
    cs.DistinctCategories,
    cat.CategoryName,
    ccs.CustomerCategorySpend,
    ca.AvgSpendPerOrder,
    ca.TotalCategoryRevenue,
    CASE 
        WHEN ccs.CustomerCategorySpend > ca.AvgSpendPerOrder THEN 'Above Avg'
        WHEN ccs.CustomerCategorySpend = ca.AvgSpendPerOrder THEN 'Avg'
        ELSE 'Below Avg'
    END AS CategorySpendComparison,
    CASE 
        WHEN ca.TotalCategoryRevenue > 100000 THEN 'High Revenue Category'
        WHEN ca.TotalCategoryRevenue > 50000 THEN 'Medium Revenue Category'
        ELSE 'Low Revenue Category'
    END AS CategoryPerformance
FROM Customers c
INNER JOIN CustomerStats cs ON c.CustomerID = cs.CustomerID
INNER JOIN CustomerCategoryStats ccs ON c.CustomerID = ccs.CustomerID
INNER JOIN CategoryAverages ca ON ccs.CategoryID = ca.CategoryID
INNER JOIN Categories cat ON ccs.CategoryID = cat.CategoryID
ORDER BY cs.LoyaltyTier DESC, cs.TotalSpent DESC, cat.CategoryName;
