
WITH EmployeeDetails AS (
    SELECT 
        e.BusinessEntityID,
        p.FirstName,
        p.LastName,
        e.JobTitle,
        e.HireDate,
        e.VacationHours,
        e.SickLeaveHours,
        e.SalariedFlag,
        dh.DepartmentID,
        d.Name AS DepartmentName,
        CASE 
            WHEN e.SalariedFlag = 1 THEN 'Salaried'
            ELSE 'Hourly'
        END AS PayType,
        CASE 
            WHEN YEAR(GETDATE()) - YEAR(e.HireDate) >= 10 THEN 'Senior'
            WHEN YEAR(GETDATE()) - YEAR(e.HireDate) >= 5 THEN 'Mid-Level'
            ELSE 'Junior'
        END AS ExperienceLevel
    FROM HumanResources.Employee e
    INNER JOIN Person.Person p ON e.BusinessEntityID = p.BusinessEntityID
    INNER JOIN HumanResources.EmployeeDepartmentHistory dh ON e.BusinessEntityID = dh.BusinessEntityID
    INNER JOIN HumanResources.Department d ON dh.DepartmentID = d.DepartmentID
    WHERE e.CurrentFlag = 1 
        AND dh.EndDate IS NULL
),
DepartmentStats AS (
    SELECT 
        DepartmentName,
        COUNT(*) AS TotalEmployees,
        COUNT(CASE WHEN PayType = 'Salaried' THEN 1 END) AS SalariedEmployees,
        COUNT(CASE WHEN PayType = 'Hourly' THEN 1 END) AS HourlyEmployees,
        COUNT(CASE WHEN ExperienceLevel = 'Senior' THEN 1 END) AS SeniorEmployees,
        COUNT(CASE WHEN ExperienceLevel = 'Mid-Level' THEN 1 END) AS MidLevelEmployees,
        COUNT(CASE WHEN ExperienceLevel = 'Junior' THEN 1 END) AS JuniorEmployees,
        AVG(VacationHours) AS AvgVacationHours,
        AVG(SickLeaveHours) AS AvgSickLeaveHours,
        MAX(VacationHours) AS MaxVacationHours,
        MIN(VacationHours) AS MinVacationHours
    FROM EmployeeDetails
    GROUP BY DepartmentName
),
PayTypeBreakdown AS (
    SELECT 
        DepartmentName,
        PayType,
        COUNT(*) AS CountByPayType,
        AVG(VacationHours) AS AvgVacationByPayType,
        AVG(SickLeaveHours) AS AvgSickLeaveByPayType
    FROM EmployeeDetails
    GROUP BY DepartmentName, PayType
)
SELECT 
    ed.DepartmentName,
    ed.FirstName,
    ed.LastName,
    ed.JobTitle,
    ed.HireDate,
    ed.PayType,
    ed.ExperienceLevel,
    ed.VacationHours,
    ed.SickLeaveHours,
    ds.TotalEmployees,
    ds.SalariedEmployees,
    ds.HourlyEmployees,
    ds.SeniorEmployees,
    ds.MidLevelEmployees,
    ds.JuniorEmployees,
    ROUND(ds.AvgVacationHours, 1) AS DeptAvgVacation,
    ROUND(ds.AvgSickLeaveHours, 1) AS DeptAvgSickLeave,
    CASE 
        WHEN ed.VacationHours > ds.AvgVacationHours THEN 'Above Average'
        WHEN ed.VacationHours = ds.AvgVacationHours THEN 'Average'
        ELSE 'Below Average'
    END AS VacationComparison,
    CASE 
        WHEN ds.TotalEmployees > 20 THEN 'Large Department'
        WHEN ds.TotalEmployees > 10 THEN 'Medium Department'
        ELSE 'Small Department'
    END AS DepartmentSize,
    ROUND((ds.SalariedEmployees * 100.0) / ds.TotalEmployees, 1) AS SalariedPercentage,
    ptb.CountByPayType,
    ROUND(ptb.AvgVacationByPayType, 1) AS PayTypeAvgVacation
FROM EmployeeDetails ed
INNER JOIN DepartmentStats ds ON ed.DepartmentName = ds.DepartmentName
INNER JOIN PayTypeBreakdown ptb ON ed.DepartmentName = ptb.DepartmentName 
    AND ed.PayType = ptb.PayType
ORDER BY ed.DepartmentName, ed.LastName, ed.FirstName;

-- ========================================
-- MEDIUM QUERY 2: Product Sales Summary by Category
-- ========================================
-- Basic product analysis with simple aggregations and categorization
WITH ProductInfo AS (
    SELECT 
        p.ProductID,
        p.Name AS ProductName,
        p.ProductNumber,
        p.ListPrice,
        p.StandardCost,
        p.Color,
        p.Size,
        ps.Name AS SubcategoryName,
        pc.Name AS CategoryName,
        CASE 
            WHEN p.ListPrice > 1000 THEN 'High-End'
            WHEN p.ListPrice > 500 THEN 'Mid-Range'
            WHEN p.ListPrice > 100 THEN 'Standard'
            ELSE 'Budget'
        END AS PriceRange,
        CASE 
            WHEN p.StandardCost > 0 THEN p.ListPrice - p.StandardCost
            ELSE 0
        END AS ProfitPerUnit
    FROM Production.Product p
    LEFT JOIN Production.ProductSubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    LEFT JOIN Production.ProductCategory pc ON ps.ProductCategoryID = pc.ProductCategoryID
    WHERE p.ListPrice > 0
),
SalesInfo AS (
    SELECT 
        sod.ProductID,
        COUNT(DISTINCT sod.SalesOrderID) AS OrderCount,
        SUM(sod.OrderQty) AS TotalQuantitySold,
        SUM(sod.LineTotal) AS TotalRevenue,
        AVG(sod.UnitPrice) AS AvgSellingPrice,
        MAX(soh.OrderDate) AS LastSaleDate,
        MIN(soh.OrderDate) AS FirstSaleDate,
        COUNT(DISTINCT soh.CustomerID) AS UniqueCustomers
    FROM Sales.SalesOrderDetail sod
    INNER JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
    WHERE soh.OrderDate >= '2013-01-01'
    GROUP BY sod.ProductID
),
CategoryTotals AS (
    SELECT 
        pi.CategoryName,
        COUNT(pi.ProductID) AS ProductsInCategory,
        SUM(si.TotalRevenue) AS CategoryRevenue,
        AVG(pi.ListPrice) AS AvgCategoryPrice,
        SUM(si.TotalQuantitySold) AS CategoryQuantitySold,
        COUNT(si.ProductID) AS ProductsWithSales
    FROM ProductInfo pi
    LEFT JOIN SalesInfo si ON pi.ProductID = si.ProductID
    WHERE pi.CategoryName IS NOT NULL
    GROUP BY pi.CategoryName
),
InventoryInfo AS (
    SELECT 
        ProductID,
        SUM(Quantity) AS TotalInventory,
        COUNT(LocationID) AS LocationCount,
        AVG(Quantity) AS AvgInventoryPerLocation
    FROM Production.ProductInventory
    GROUP BY ProductID
)
SELECT 
    pi.CategoryName,
    pi.SubcategoryName,
    pi.ProductName,
    pi.ProductNumber,
    pi.ListPrice,
    pi.StandardCost,
    pi.ProfitPerUnit,
    pi.PriceRange,
    pi.Color,
    pi.Size,
    ISNULL(si.TotalQuantitySold, 0) AS QuantitySold,
    ISNULL(si.TotalRevenue, 0) AS Revenue,
    ISNULL(si.OrderCount, 0) AS OrderCount,
    ISNULL(si.UniqueCustomers, 0) AS UniqueCustomers,
    si.LastSaleDate,
    ISNULL(ii.TotalInventory, 0) AS CurrentInventory,
    ISNULL(ii.LocationCount, 0) AS InventoryLocations,
    ct.CategoryRevenue,
    ct.ProductsInCategory,
    ct.ProductsWithSales,
    ROUND(ct.AvgCategoryPrice, 2) AS AvgCategoryPrice,
    CASE 
        WHEN si.TotalQuantitySold IS NULL THEN 'No Sales'
        WHEN si.TotalQuantitySold > 1000 THEN 'High Volume'
        WHEN si.TotalQuantitySold > 100 THEN 'Medium Volume'
        ELSE 'Low Volume'
    END AS SalesVolume,
    CASE 
        WHEN si.LastSaleDate >= DATEADD(MONTH, -6, GETDATE()) THEN 'Recent'
        WHEN si.LastSaleDate >= DATEADD(YEAR, -1, GETDATE()) THEN 'Moderate'
        WHEN si.LastSaleDate IS NOT NULL THEN 'Old'
        ELSE 'Never Sold'
    END AS SalesRecency,
    CASE 
        WHEN ii.TotalInventory > 1000 THEN 'High Stock'
        WHEN ii.TotalInventory > 100 THEN 'Medium Stock'
        WHEN ii.TotalInventory > 0 THEN 'Low Stock'
        ELSE 'No Stock'
    END AS InventoryLevel,
    ROUND((ISNULL(si.TotalRevenue, 0) * 100.0) / NULLIF(ct.CategoryRevenue, 0), 2) AS PercentOfCategoryRevenue
FROM ProductInfo pi
LEFT JOIN SalesInfo si ON pi.ProductID = si.ProductID
LEFT JOIN CategoryTotals ct ON pi.CategoryName = ct.CategoryName
LEFT JOIN InventoryInfo ii ON pi.ProductID = ii.ProductID
WHERE pi.CategoryName IS NOT NULL
ORDER BY pi.CategoryName, si.TotalRevenue DESC, pi.ProductName;
