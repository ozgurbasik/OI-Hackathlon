WITH ProductMetrics AS (
    SELECT 
        p.ProductID,
        p.Name AS ProductName,
        p.ProductNumber,
        ps.Name AS SubcategoryName,
        pc.Name AS CategoryName,
        p.ListPrice,
        p.StandardCost,
        p.ListPrice - p.StandardCost AS GrossProfit,
        CASE 
            WHEN p.ListPrice > 0 
            THEN ROUND(((p.ListPrice - p.StandardCost) / p.ListPrice) * 100, 2)
            ELSE 0 
        END AS GrossProfitMargin,
        p.SafetyStockLevel,
        p.ReorderPoint,
        p.DaysToManufacture
    FROM Production.Product p
    LEFT JOIN Production.ProductSubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    LEFT JOIN Production.ProductCategory pc ON ps.ProductCategoryID = pc.ProductCategoryID
    WHERE p.ListPrice > 0
),
SalesData AS (
    SELECT 
        sod.ProductID,
        COUNT(DISTINCT sod.SalesOrderID) AS OrderCount,
        SUM(sod.OrderQty) AS TotalQuantitySold,
        SUM(sod.LineTotal) AS TotalRevenue,
        AVG(sod.UnitPrice) AS AvgSellingPrice,
        MAX(soh.OrderDate) AS LastSaleDate,
        MIN(soh.OrderDate) AS FirstSaleDate,
        COUNT(DISTINCT YEAR(soh.OrderDate)) AS SaleYears,
        COUNT(DISTINCT soh.CustomerID) AS UniqueCustomers,
        STDEV(sod.OrderQty) AS QtyStandardDeviation
    FROM Sales.SalesOrderDetail sod
    INNER JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
    WHERE soh.OrderDate >= '2012-01-01'
    GROUP BY sod.ProductID
),
InventoryData AS (
    SELECT 
        pi.ProductID,
        SUM(pi.Quantity) AS TotalInventory,
        COUNT(pi.LocationID) AS LocationCount,
        AVG(pi.Quantity) AS AvgInventoryPerLocation,
        MAX(pi.Quantity) AS MaxLocationInventory,
        MIN(pi.Quantity) AS MinLocationInventory
    FROM Production.ProductInventory pi
    GROUP BY pi.ProductID
),
CategoryAnalysis AS (
    SELECT 
        pm.CategoryName,
        COUNT(pm.ProductID) AS ProductCount,
        AVG(pm.GrossProfitMargin) AS AvgMargin,
        SUM(sd.TotalRevenue) AS CategoryRevenue,
        AVG(sd.TotalQuantitySold) AS AvgQuantityPerProduct,
        MAX(sd.TotalRevenue) AS TopProductRevenue,
        MIN(sd.TotalRevenue) AS LowestProductRevenue,
        COUNT(CASE WHEN sd.LastSaleDate >= DATEADD(MONTH, -6, GETDATE()) THEN 1 END) AS RecentlyActivProducts
    FROM ProductMetrics pm
    LEFT JOIN SalesData sd ON pm.ProductID = sd.ProductID
    GROUP BY pm.CategoryName
)
SELECT 
    pm.CategoryName,
    pm.SubcategoryName,
    pm.ProductName,
    pm.ProductNumber,
    pm.ListPrice,
    pm.StandardCost,
    pm.GrossProfit,
    pm.GrossProfitMargin,
    sd.TotalQuantitySold,
    sd.TotalRevenue,
    sd.AvgSellingPrice,
    sd.OrderCount,
    sd.UniqueCustomers,
    id.TotalInventory,
    id.LocationCount,
    CASE 
        WHEN sd.TotalQuantitySold > 0 AND id.TotalInventory > 0
        THEN ROUND(id.TotalInventory / (sd.TotalQuantitySold / 365.0), 1)
        ELSE NULL 
    END AS DaysOfInventory,
    CASE 
        WHEN sd.TotalQuantitySold IS NULL THEN 'No Sales'
        WHEN sd.LastSaleDate < DATEADD(YEAR, -1, GETDATE()) THEN 'Discontinued'
        WHEN sd.TotalQuantitySold < 100 THEN 'Low Volume'
        WHEN sd.TotalQuantitySold < 1000 THEN 'Medium Volume'
        ELSE 'High Volume'
    END AS SalesCategory,
    CASE 
        WHEN pm.GrossProfitMargin > 50 THEN 'High Margin'
        WHEN pm.GrossProfitMargin > 30 THEN 'Good Margin'
        WHEN pm.GrossProfitMargin > 10 THEN 'Low Margin'
        ELSE 'Poor Margin'
    END AS MarginCategory,
    ca.CategoryRevenue,
    ca.AvgMargin AS CategoryAvgMargin,
    RANK() OVER (PARTITION BY pm.CategoryName ORDER BY sd.TotalRevenue DESC) AS CategoryRank,
    RANK() OVER (ORDER BY sd.TotalRevenue DESC) AS OverallRank,
    CASE 
        WHEN RANK() OVER (PARTITION BY pm.CategoryName ORDER BY sd.TotalRevenue DESC) <= 3 
        THEN 'Top 3 in Category'
        WHEN RANK() OVER (PARTITION BY pm.CategoryName ORDER BY sd.TotalRevenue DESC) <= 10 
        THEN 'Top 10 in Category'
        ELSE 'Other'
    END AS CategoryPosition,
    (SELECT TOP 1 ProductName 
     FROM ProductMetrics pm2 
     JOIN SalesData sd2 ON pm2.ProductID = sd2.ProductID
     WHERE pm2.CategoryName = pm.CategoryName 
     ORDER BY sd2.TotalRevenue DESC) AS CategoryTopProduct
FROM ProductMetrics pm
LEFT JOIN SalesData sd ON pm.ProductID = sd.ProductID
LEFT JOIN InventoryData id ON pm.ProductID = id.ProductID
LEFT JOIN CategoryAnalysis ca ON pm.CategoryName = ca.CategoryName
WHERE pm.CategoryName IS NOT NULL
ORDER BY pm.CategoryName, sd.TotalRevenue DESC;