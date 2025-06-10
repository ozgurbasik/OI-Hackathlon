WITH ProductSales AS (
    SELECT
        p.ProductID,
        p.ProductName,
        p.CategoryID,
        p.UnitPrice,
        p.UnitsInStock,
        od.OrderID,
        od.Quantity,
        od.UnitPrice AS SalePrice,
        od.Discount,
        o.OrderDate,
        o.CustomerID,
        c.CompanyName,
        c.Country,
        cat.CategoryName,
        s.CompanyName AS SupplierName,
        (od.Quantity * od.UnitPrice * (1 - od.Discount)) AS NetSaleAmount
    FROM Products p
    INNER JOIN OrderDetails od ON p.ProductID = od.ProductID
    INNER JOIN Orders o ON od.OrderID = o.OrderID
    INNER JOIN Customers c ON o.CustomerID = c.CustomerID
    INNER JOIN Categories cat ON p.CategoryID = cat.CategoryID
    INNER JOIN Suppliers s ON p.SupplierID = s.SupplierID
),
ProductStats AS (
    SELECT
        ProductID,
        SUM(NetSaleAmount) AS TotalRevenue,
        COUNT(DISTINCT OrderID) AS TotalOrders,
        COUNT(DISTINCT CustomerID) AS UniqueCustomers,
        SUM(Quantity) AS TotalQuantitySold,
        AVG(NetSaleAmount) AS AvgOrderValue,
        CASE
            WHEN SUM(NetSaleAmount) >= 50000 THEN 'Top Performer'
            WHEN SUM(NetSaleAmount) >= 25000 THEN 'Strong Seller'
            WHEN SUM(NetSaleAmount) >= 10000 THEN 'Moderate Seller'
            ELSE 'Low Performer'
        END AS SalesCategory
    FROM ProductSales
    GROUP BY ProductID
),
CategoryAverages AS (
    SELECT
        CategoryID,
        CategoryName,
        AVG(NetSaleAmount) AS AvgSalePerOrder,
        AVG(Quantity) AS AvgQuantityPerOrder,
        SUM(NetSaleAmount) AS TotalCategoryRevenue,
        COUNT(DISTINCT ProductID) AS ProductsInCategory
    FROM ProductSales
    GROUP BY CategoryID, CategoryName
),
ProductCategoryStats AS (
    SELECT
        ps.ProductID,
        ps.CategoryID,
        ps.CategoryName,
        SUM(ps.NetSaleAmount) AS ProductCategoryRevenue,
        COUNT(DISTINCT ps.CustomerID) AS CustomersInCategory,
        AVG(ps.Discount) AS AvgDiscountUsed
    FROM ProductSales ps
    GROUP BY ps.ProductID, ps.CategoryID, ps.CategoryName
)
SELECT
    p.ProductName,
    p.UnitPrice,
    p.UnitsInStock,
    ps.SalesCategory,
    ps.TotalRevenue,
    ps.TotalOrders,
    ps.UniqueCustomers,
    ps.TotalQuantitySold,
    cat.CategoryName,
    pcs.ProductCategoryRevenue,
    ca.AvgSalePerOrder,
    ca.TotalCategoryRevenue,
    s.CompanyName AS SupplierName,
    ROUND((ps.TotalQuantitySold * 100.0 / NULLIF(p.UnitsInStock + ps.TotalQuantitySold, 0)), 2) AS StockTurnoverRate,
    CASE
        WHEN pcs.ProductCategoryRevenue > ca.AvgSalePerOrder THEN 'Above Category Avg'
        WHEN pcs.ProductCategoryRevenue = ca.AvgSalePerOrder THEN 'At Category Avg'
        ELSE 'Below Category Avg'
    END AS CategoryPerformanceComparison,
    CASE
        WHEN ca.TotalCategoryRevenue > 200000 THEN 'High Revenue Category'
        WHEN ca.TotalCategoryRevenue > 100000 THEN 'Medium Revenue Category'
        ELSE 'Low Revenue Category'
    END AS CategoryClassification,
    CASE
        WHEN ps.UniqueCustomers > 50 AND ps.TotalRevenue > 30000 THEN 'Market Leader'
        WHEN ps.UniqueCustomers > 25 AND ps.TotalRevenue > 15000 THEN 'Strong Product'
        WHEN ps.UniqueCustomers > 10 THEN 'Growing Product'
        ELSE 'Niche Product'
    END AS MarketPosition,
    CASE
        WHEN p.UnitsInStock < 10 AND ps.TotalQuantitySold > 100 THEN 'Reorder Required'
        WHEN p.UnitsInStock < 25 AND ps.TotalQuantitySold > 50 THEN 'Monitor Stock'
        ELSE 'Stock OK'
    END AS StockStatus
FROM Products p
INNER JOIN ProductStats ps ON p.ProductID = ps.ProductID
INNER JOIN ProductCategoryStats pcs ON p.ProductID = pcs.ProductID
INNER JOIN CategoryAverages ca ON pcs.CategoryID = ca.CategoryID
INNER JOIN Categories cat ON pcs.CategoryID = cat.CategoryID
INNER JOIN Suppliers s ON p.SupplierID = s.SupplierID
ORDER BY ps.SalesCategory DESC, ps.TotalRevenue DESC, cat.CategoryName;