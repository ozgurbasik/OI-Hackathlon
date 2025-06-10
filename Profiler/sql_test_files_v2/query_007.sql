 WITH
  -- 1. Calculate monthly sales totals
  MonthlySales AS (
  SELECT
  YEAR(soh.OrderDate) AS SalesYear,
  MONTH(soh.OrderDate) AS SalesMonth,
  SUM(sod.LineTotal) AS TotalSales
  FROM
  Sales.SalesOrderHeader soh
  JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
  GROUP BY
  YEAR(soh.OrderDate),
  MONTH(soh.OrderDate)
  ),
  -- 2. Calculate the average monthly sales
  AverageMonthlySales AS (
  SELECT
  AVG(TotalSales) AS AverageSales
  FROM
  MonthlySales
  ),
  -- 3. Identify high-value customers (customers with total purchases above the average)
  HighValueCustomers AS (
  SELECT
  soh.CustomerID,
  SUM(sod.LineTotal) AS TotalPurchaseAmount
  FROM
  Sales.SalesOrderHeader soh
  JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
  GROUP BY
  soh.CustomerID
  HAVING
  SUM(sod.LineTotal) > (
  SELECT
  AverageSales
  FROM
  AverageMonthlySales
  )
  ),
  -- 4. Rank products by sales volume
  ProductRanking AS (
  SELECT
  p.ProductID,
  p.Name AS ProductName,
  SUM(sod.LineTotal) AS TotalSales,
  RANK() OVER (
  ORDER BY
  SUM(sod.LineTotal) DESC
  ) AS SalesRank
  FROM
  Production.Product p
  JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
  GROUP BY
  p.ProductID,
  p.Name
  ),
  -- 5. Calculate the percentage of sales by product category
  CategorySalesPercentage AS (
  SELECT
  pc.Name AS CategoryName,
  SUM(sod.LineTotal) AS CategorySales,
  (
  SUM(sod.LineTotal) * 100.0 / (
  SELECT
  CASE
  WHEN SUM(LineTotal) = 0 THEN 1 -- Prevent division by zero
  ELSE SUM(LineTotal)
  END
  FROM
  Sales.SalesOrderDetail
  )
  ) AS SalesPercentage
  FROM
  Production.ProductCategory pc
  JOIN Production.ProductSubcategory psc ON pc.ProductCategoryID = psc.ProductCategoryID
  JOIN Production.Product p ON psc.ProductSubcategoryID = p.ProductSubcategoryID
  JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
  GROUP BY
  pc.Name
  ),
  -- 6. Determine the top selling product in each category
  TopProductInCategory AS (
  SELECT
  pc.Name AS CategoryName,
  p.Name AS ProductName,
  SUM(sod.LineTotal) AS TotalSales,
  ROW_NUMBER() OVER (
  PARTITION BY
  pc.Name
  ORDER BY
  SUM(sod.LineTotal) DESC
  ) AS RowNum
  FROM
  Production.ProductCategory pc
  JOIN Production.ProductSubcategory psc ON pc.ProductCategoryID = psc.ProductCategoryID
  JOIN Production.Product p ON psc.ProductSubcategoryID = p.ProductSubcategoryID
  JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
  GROUP BY
  pc.Name,
  p.Name
  ),
  -- 7. Calculate year-over-year sales growth
  YearlySales AS (
  SELECT
  YEAR(soh.OrderDate) AS SalesYear,
  SUM(sod.LineTotal) AS TotalSales
  FROM
  Sales.SalesOrderHeader soh
  JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
  GROUP BY
  YEAR(soh.OrderDate)
  ),
  YearOverYearGrowth AS (
  SELECT
  SalesYear,
  TotalSales,
  LAG(TotalSales, 1, 0) OVER (
  ORDER BY
  SalesYear
  ) AS PreviousYearSales,
  CASE
  WHEN LAG(TotalSales, 1, 0) OVER (
  ORDER BY
  SalesYear
  ) = 0 THEN 0 -- Handle cases where previous year sales are zero
  ELSE (TotalSales - LAG(TotalSales, 1, 0) OVER (
  ORDER BY
  SalesYear
  )) * 100.0 / LAG(TotalSales, 1, 0) OVER (
  ORDER BY
  SalesYear
  )
  END AS GrowthPercentage
  FROM
  YearlySales
  ),
  -- 8. Analyze customer demographics (using dummy data since AdventureWorks lacks detailed demographics)
  CustomerDemographics AS (
  SELECT TOP 100 PERCENT
  c.CustomerID,
  CASE
  WHEN c.CustomerID % 2 = 0 THEN 'Male'
  ELSE 'Female'
  END AS Gender,
  CASE
  WHEN c.CustomerID % 5 = 0 THEN '25-34'
  WHEN c.CustomerID % 5 = 1 THEN '35-44'
  WHEN c.CustomerID % 5 = 2 THEN '45-54'
  ELSE '55+'
  END AS AgeGroup,
  CASE
  WHEN c.CustomerID % 3 = 0 THEN 'High'
  WHEN c.CustomerID % 3 = 1 THEN 'Medium'
  ELSE 'Low'
  END AS IncomeLevel
  FROM
  Sales.Customer c
  ORDER BY c.CustomerID
  ),
  -- 9. Combine customer demographics with purchase data
  CustomerPurchaseAnalysis AS (
  SELECT
  cd.Gender,
  cd.AgeGroup,
  cd.IncomeLevel,
  SUM(soh.TotalDue) AS TotalPurchases
  FROM
  CustomerDemographics cd
  JOIN Sales.SalesOrderHeader soh ON cd.CustomerID = soh.CustomerID
  GROUP BY
  cd.Gender,
  cd.AgeGroup,
  cd.IncomeLevel
  ),
  -- 10. Calculate shipping times
  ShippingTimes AS (
  SELECT
  soh.SalesOrderID,
  soh.OrderDate,
  soh.ShipDate,
  DATEDIFF(day, soh.OrderDate, soh.ShipDate) AS ShippingDays
  FROM
  Sales.SalesOrderHeader soh
  WHERE
  soh.ShipDate IS NOT NULL
  ),
  -- 11. Average shipping time by territory
  AvgShippingByTerritory AS (
  SELECT
  st.Name AS TerritoryName,
  AVG(st2.ShippingDays) AS AvgShippingDays
  FROM
  Sales.SalesTerritory st
  JOIN Sales.SalesOrderHeader soh ON st.TerritoryID = soh.TerritoryID
  JOIN ShippingTimes st2 ON soh.SalesOrderID = st2.SalesOrderID
  GROUP BY
  st.Name
  )
  
  
  -- Final SELECT statement to combine and present the results
 SELECT
  ms.SalesYear,
  ms.SalesMonth,
  ms.TotalSales AS MonthlySalesTotal,
  ams.AverageSales AS OverallAverageMonthlySales,
  (
  SELECT
  COUNT(*)
  FROM
  HighValueCustomers
  ) AS NumberOfHighValueCustomers,
  pr.ProductName AS TopSellingProduct,
  pr.TotalSales AS TopSellingProductSales,
  csp.CategoryName AS HighestSalesCategory,
  csp.SalesPercentage AS HighestSalesCategoryPercentage,
  tpic.ProductName AS TopProductInTopCategory,
  yy.GrowthPercentage AS YearOverYearSalesGrowth,
  cpa.Gender AS CustomerGender,
  cpa.AgeGroup AS CustomerAgeGroup,
  cpa.IncomeLevel AS CustomerIncomeLevel,
  cpa.TotalPurchases AS TotalCustomerPurchases,
  ast.TerritoryName AS TerritoryName,
  ast.AvgShippingDays AS AverageShippingDays
 FROM
  MonthlySales ms
  CROSS JOIN AverageMonthlySales ams
  CROSS JOIN (
  SELECT TOP 1
  ProductName,
  TotalSales
  FROM
  ProductRanking
  ORDER BY
  TotalSales DESC
  ) pr
  CROSS JOIN (
  SELECT TOP 1
  CategoryName,
  SalesPercentage
  FROM
  CategorySalesPercentage
  ORDER BY
  SalesPercentage DESC
  ) csp
  CROSS JOIN (
  SELECT TOP 1
  CategoryName,
  ProductName
  FROM
  TopProductInCategory
  ORDER BY
  TotalSales DESC
  ) tpic
  CROSS JOIN YearOverYearGrowth yy
  CROSS JOIN (
  SELECT TOP 1
  Gender,
  AgeGroup,
  IncomeLevel,
  TotalPurchases
  FROM
  CustomerPurchaseAnalysis
  ORDER BY
  TotalPurchases DESC
  ) cpa
  CROSS JOIN (
  SELECT TOP 1
  TerritoryName,
  AvgShippingDays
  FROM
  AvgShippingByTerritory
  ORDER BY
  AvgShippingDays ASC
  ) ast
 ORDER BY
  ms.SalesYear,
  ms.SalesMonth;