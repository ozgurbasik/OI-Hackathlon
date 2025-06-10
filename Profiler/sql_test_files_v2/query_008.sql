 WITH
  -- 1. Calculate current inventory value for each product
  InventoryValue AS (
  SELECT
  p.ProductID,
  p.Name AS ProductName,
  SUM(inv.Quantity * p.StandardCost) AS TotalInventoryValue,
  SUM(inv.Quantity) AS TotalInventoryQuantity
  FROM
  Production.Product p
  JOIN Production.ProductInventory inv ON p.ProductID = inv.ProductID
  GROUP BY
  p.ProductID,
  p.Name
  ),
  -- 2. Identify products below reorder point
  LowStockProducts AS (
  SELECT
  p.ProductID,
  p.Name AS ProductName,
  p.ReorderPoint,
  SUM(inv.Quantity) AS CurrentQuantity
  FROM
  Production.Product p
  JOIN Production.ProductInventory inv ON p.ProductID = inv.ProductID
  GROUP BY
  p.ProductID,
  p.Name,
  p.ReorderPoint
  HAVING
  SUM(inv.Quantity) < p.ReorderPoint
  ),
  -- 3. Calculate average lead time from suppliers
  SupplierLeadTime AS (
  SELECT
  po.VendorID,
  v.Name AS VendorName,
  AVG(DATEDIFF(day, po.OrderDate, po.ShipDate)) AS AverageLeadTime
  FROM
  Purchasing.PurchaseOrderHeader po
  JOIN Purchasing.Vendor v ON po.VendorID = v.BusinessEntityID
  WHERE
  po.ShipDate IS NOT NULL
  GROUP BY
  po.VendorID,
  v.Name
  ),
  -- 4. Rank suppliers by on-time delivery performance
  OnTimeDeliveryRanking AS (
  SELECT
  po.VendorID,
  v.Name AS VendorName,
  SUM(
  CASE
  WHEN po.ShipDate <= po.ModifiedDate THEN 1  -- Assuming ShipDate before ModifiedDate means on-time
  ELSE 0
  END
  ) AS OnTimeDeliveries,
  COUNT(*) AS TotalDeliveries,
  CASE
  WHEN COUNT(*) = 0 THEN 0  -- Prevent division by zero
  ELSE CAST(SUM(
  CASE
  WHEN po.ShipDate <= po.ModifiedDate THEN 1
  ELSE 0
  END
  ) AS FLOAT) * 100 / COUNT(*)
  END AS OnTimeDeliveryPercentage,
  RANK() OVER (
  ORDER BY
  CASE
  WHEN COUNT(*) = 0 THEN 0  -- Prevent ranking issues
  ELSE CAST(SUM(
  CASE
  WHEN po.ShipDate <= po.ModifiedDate THEN 1
  ELSE 0
  END
  ) AS FLOAT) * 100 / COUNT(*)
  END DESC
  ) AS DeliveryRank
  FROM
  Purchasing.PurchaseOrderHeader po
  JOIN Purchasing.Vendor v ON po.VendorID = v.BusinessEntityID
  WHERE po.ShipDate IS NOT NULL -- Only consider shipped orders
  GROUP BY
  po.VendorID,
  v.Name
  ),
  -- 5. Calculate product demand based on sales history (monthly)
  ProductDemandMonthly AS (
  SELECT
  p.ProductID,
  p.Name AS ProductName,
  YEAR(soh.OrderDate) AS SalesYear,
  MONTH(soh.OrderDate) AS SalesMonth,
  SUM(sod.OrderQty) AS TotalQuantitySold -- Corrected column name
  FROM
  Production.Product p
  JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
  JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
  GROUP BY
  p.ProductID,
  p.Name,
  YEAR(soh.OrderDate),
  MONTH(soh.OrderDate)
  ),
  -- 6. Calculate product demand based on sales history (overall)
  ProductDemand AS (
  SELECT
  p.ProductID,
  p.Name AS ProductName,
  SUM(sod.OrderQty) AS TotalQuantitySold -- Corrected column name
  FROM
  Production.Product p
  JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
  GROUP BY
  p.ProductID,
  p.Name
  ),
  -- 7. Identify slow-moving products (products with low sales and high inventory)
  SlowMovingProducts AS (
  SELECT
  iv.ProductID,
  iv.ProductName,
  iv.TotalInventoryQuantity,
  pd.TotalQuantitySold
  FROM
  InventoryValue iv
  JOIN ProductDemand pd ON iv.ProductID = pd.ProductID
  WHERE
  pd.TotalQuantitySold < 100 -- Example threshold: adjust as needed
  AND iv.TotalInventoryQuantity > 500 -- Example threshold: adjust as needed
  ),
  -- 8. Calculate safety stock levels (more advanced - requires historical data)
  SafetyStock AS (
  SELECT
  p.ProductID,
  p.Name AS ProductName,
  /*
  More advanced safety stock calculation:
  Safety Stock = Z * sqrt(Lead Time Standard Deviation^2 * Average Demand^2 + Demand Standard Deviation^2 * Average Lead Time^2)
  Where Z is the service level factor (e.g., 1.645 for 95% service level).
  This requires calculating standard deviations of lead time and demand, which is beyond the scope of this simplified example.
  For this example, we use a fixed percentage of average demand, adjusted by lead time.
  */
  ProductDemand.TotalQuantitySold * 0.2 * (
  1 + (
  SELECT
  AVG(AverageLeadTime)
  FROM
  SupplierLeadTime
  ) / 30
  ) AS SafetyStockLevel -- 20% of average demand, adjusted by average lead time
  FROM
  Production.Product p
  JOIN ProductDemand ON p.ProductID = ProductDemand.ProductID
  ),
  -- 9. Analyze purchase order amounts by vendor
  PurchaseOrderAmounts AS (
  SELECT
  po.VendorID,
  v.Name AS VendorName,
  SUM(po.TotalDue) AS TotalPurchaseAmount
  FROM
  Purchasing.PurchaseOrderHeader po
  JOIN Purchasing.Vendor v ON po.VendorID = v.BusinessEntityID
  GROUP BY
  po.VendorID,
  v.Name
  ),
  -- 10. Identify preferred vendors (top vendors by purchase amount)
  PreferredVendors AS (
  SELECT
  VendorID,
  VendorName,
  TotalPurchaseAmount,
  RANK() OVER (
  ORDER BY
  TotalPurchaseAmount DESC
  ) AS VendorRank
  FROM
  PurchaseOrderAmounts
  ),
  -- 11. Calculate inventory turnover rate
  InventoryTurnover AS (
  SELECT
  iv.ProductID,
  iv.ProductName,
  pd.TotalQuantitySold,
  iv.TotalInventoryValue,
  CASE
  WHEN iv.TotalInventoryValue = 0 THEN 0  -- Prevent division by zero
  ELSE pd.TotalQuantitySold * (
  SELECT
  AVG(StandardCost)
  FROM
  Production.Product
  WHERE
  ProductID = iv.ProductID
  ) / iv.TotalInventoryValue
  END AS TurnoverRate
  FROM
  InventoryValue iv
  JOIN ProductDemand pd ON iv.ProductID = pd.ProductID
  ),
  -- 12. Forecast future demand (simplified moving average)
  DemandForecast AS (
  SELECT
  ProductID,
  ProductName,
  AVG(TotalQuantitySold) AS ForecastedDemand
  FROM
  ProductDemandMonthly
  WHERE
  SalesYear >= YEAR(GETDATE()) - 1 -- Use last year's data for forecasting
  GROUP BY
  ProductID,
  ProductName
  ),
  -- 13. Calculate Economic Order Quantity (EOQ) - Simplified
  EconomicOrderQuantity AS (
  SELECT
  p.ProductID,
  p.Name AS ProductName,
  /*
  Simplified EOQ calculation:
  EOQ = sqrt((2 * Annual Demand * Order Cost) / Holding Cost per Unit)
  Requires estimating order cost and holding cost per unit.
  For this example, we use fixed values.
  */
  SQRT(
  (
  2 * ProductDemand.TotalQuantitySold * 100
  ) / (p.StandardCost * 0.1)
  ) AS EOQ
  FROM
  Production.Product p
  JOIN ProductDemand ON p.ProductID = ProductDemand.ProductID
  ),
  -- 14. Analyze inventory holding costs (simplified)
  InventoryHoldingCost AS (
  SELECT
  iv.ProductID,
  iv.ProductName,
  iv.TotalInventoryValue * 0.1 AS HoldingCost -- 10% of inventory value
  FROM
  InventoryValue iv
  ),
  -- 15. Identify products with high holding costs and low turnover
  HighCostLowTurnover AS (
  SELECT
  ihc.ProductID,
  ihc.ProductName,
  ihc.HoldingCost,
  ito.TurnoverRate
  FROM
  InventoryHoldingCost ihc
  JOIN InventoryTurnover ito ON ihc.ProductID = ito.ProductID
  WHERE
  ihc.HoldingCost > 1000
  AND ito.TurnoverRate < 1
  )
  
  
  -- Final SELECT statement to combine and present the results
 SELECT
  iv.ProductName,
  iv.TotalInventoryValue,
  lsp.ProductName AS LowStockProductName,
  otd.VendorName AS TopVendor,
  otd.OnTimeDeliveryPercentage,
  pd.TotalQuantitySold,
  smp.ProductName AS SlowMovingProductName,
  ss.SafetyStockLevel,
  pv.VendorName AS PreferredVendor,
  ito.TurnoverRate,
  df.ForecastedDemand,
  eoq.EOQ,
  ihc.HoldingCost,
  hclt.ProductName AS HighCostLowTurnoverProduct
 FROM
  InventoryValue iv
  LEFT JOIN LowStockProducts lsp ON iv.ProductID = lsp.ProductID
  LEFT JOIN (
  SELECT TOP 1
  VendorName,
  OnTimeDeliveryPercentage
  FROM
  OnTimeDeliveryRanking
  ORDER BY
  OnTimeDeliveryPercentage DESC
  ) otd ON 1 = 1
  LEFT JOIN ProductDemand pd ON iv.ProductID = pd.ProductID
  LEFT JOIN SlowMovingProducts smp ON iv.ProductID = smp.ProductID
  LEFT JOIN SafetyStock ss ON iv.ProductID = ss.ProductID
  LEFT JOIN (
  SELECT TOP 1
  VendorName
  FROM
  PreferredVendors
  ORDER BY
  TotalPurchaseAmount DESC
  ) pv ON 1 = 1
  LEFT JOIN InventoryTurnover ito ON iv.ProductID = ito.ProductID
  LEFT JOIN DemandForecast df ON iv.ProductID = df.ProductID
  LEFT JOIN EconomicOrderQuantity eoq ON iv.ProductID = eoq.ProductID
  LEFT JOIN InventoryHoldingCost ihc ON iv.ProductID = ihc.ProductID
  LEFT JOIN HighCostLowTurnover hclt ON iv.ProductID = hclt.ProductID
 ORDER BY
  iv.TotalInventoryValue DESC;