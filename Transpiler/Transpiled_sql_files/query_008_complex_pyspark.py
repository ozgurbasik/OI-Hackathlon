"""
PySpark code transpiled from query_008.sql
Classification: Complex
Generated on: 2025-06-10 19:29:07
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'

# Define the base path for the Parquet files
base_path = "/mnt/advdata/AdventureWorks/"

# 1. Calculate current inventory value for each product
Production_Product = spark.read.parquet(base_path + "Production_Product.parquet")
Production_ProductInventory = spark.read.parquet(base_path + "Production_ProductInventory.parquet")

inventory_value = Production_Product.alias("p").join(
    Production_ProductInventory.alias("inv"),
    F.col("p.ProductID") == F.col("inv.ProductID"),
    "inner"
).groupBy(
    F.col("p.ProductID"),
    F.col("p.Name")
).agg(
    F.sum(F.col("inv.Quantity") * F.col("p.StandardCost")).alias("TotalInventoryValue"),
    F.sum(F.col("inv.Quantity")).alias("TotalInventoryQuantity")
).select(
    F.col("ProductID"),
    F.col("Name").alias("ProductName"),
    F.col("TotalInventoryValue"),
    F.col("TotalInventoryQuantity")
)

# 2. Identify products below reorder point
low_stock_products = Production_Product.alias("p").join(
    Production_ProductInventory.alias("inv"),
    F.col("p.ProductID") == F.col("inv.ProductID"),
    "inner"
).groupBy(
    F.col("p.ProductID"),
    F.col("p.Name"),
    F.col("p.ReorderPoint")
).agg(
    F.sum(F.col("inv.Quantity")).alias("CurrentQuantity")
).where(
    F.col("CurrentQuantity") < F.col("ReorderPoint")
).select(
    F.col("ProductID"),
    F.col("Name").alias("ProductName"),
    F.col("ReorderPoint"),
    F.col("CurrentQuantity")
)

# 3. Calculate average lead time from suppliers
Purchasing_PurchaseOrderHeader = spark.read.parquet(base_path + "Purchasing_PurchaseOrderHeader.parquet")
Purchasing_Vendor = spark.read.parquet(base_path + "Purchasing_Vendor.parquet")

supplier_lead_time = Purchasing_PurchaseOrderHeader.alias("po").join(
    Purchasing_Vendor.alias("v"),
    F.col("po.VendorID") == F.col("v.BusinessEntityID"),
    "inner"
).where(
    F.col("po.ShipDate").isNotNull()
).groupBy(
    F.col("po.VendorID"),
    F.col("v.Name")
).agg(
    F.avg(F.datediff(F.col("po.ShipDate"), F.col("po.OrderDate"))).alias("AverageLeadTime")
).select(
    F.col("VendorID"),
    F.col("Name").alias("VendorName"),
    F.col("AverageLeadTime")
)

# 4. Rank suppliers by on-time delivery performance
on_time_delivery_ranking = Purchasing_PurchaseOrderHeader.alias("po").join(
    Purchasing_Vendor.alias("v"),
    F.col("po.VendorID") == F.col("v.BusinessEntityID"),
    "inner"
).where(
    F.col("po.ShipDate").isNotNull()
).groupBy(
    F.col("po.VendorID"),
    F.col("v.Name")
).agg(
    F.sum(F.when(F.col("po.ShipDate") <= F.col("po.ModifiedDate"), 1).otherwise(0)).alias("OnTimeDeliveries"),
    F.count("*").alias("TotalDeliveries")
).withColumn(
    "OnTimeDeliveryPercentage",
    F.when(F.col("TotalDeliveries") == 0, 0).otherwise(F.col("OnTimeDeliveries") * 100 / F.col("TotalDeliveries"))
).withColumn(
    "DeliveryRank",
    F.rank().over(Window.orderBy(F.when(F.col("TotalDeliveries") == 0, 0).otherwise(F.col("OnTimeDeliveries") * 100 / F.col("TotalDeliveries")).desc()))
).select(
    F.col("VendorID"),
    F.col("Name").alias("VendorName"),
    F.col("OnTimeDeliveries"),
    F.col("TotalDeliveries"),
    F.col("OnTimeDeliveryPercentage"),
    F.col("DeliveryRank")
)

# 5. Calculate product demand based on sales history (monthly)
Sales_SalesOrderDetail = spark.read.parquet(base_path + "Sales_SalesOrderDetail.parquet")
Sales_SalesOrderHeader = spark.read.parquet(base_path + "Sales_SalesOrderHeader.parquet")

product_demand_monthly = Production_Product.alias("p").join(
    Sales_SalesOrderDetail.alias("sod"),
    F.col("p.ProductID") == F.col("sod.ProductID"),
    "inner"
).join(
    Sales_SalesOrderHeader.alias("soh"),
    F.col("sod.SalesOrderID") == F.col("soh.SalesOrderID"),
    "inner"
).groupBy(
    F.col("p.ProductID"),
    F.col("p.Name"),
    F.year(F.col("soh.OrderDate")),
    F.month(F.col("soh.OrderDate"))
).agg(
    F.sum(F.col("sod.OrderQty")).alias("TotalQuantitySold")
).select(
    F.col("ProductID"),
    F.col("Name").alias("ProductName"),
    F.col("year(soh.OrderDate)").alias("SalesYear"),
    F.col("month(soh.OrderDate)").alias("SalesMonth"),
    F.col("TotalQuantitySold")
)

# 6. Calculate product demand based on sales history (overall)
product_demand = Production_Product.alias("p").join(
    Sales_SalesOrderDetail.alias("sod"),
    F.col("p.ProductID") == F.col("sod.ProductID"),
    "inner"
).groupBy(
    F.col("p.ProductID"),
    F.col("p.Name")
).agg(
    F.sum(F.col("sod.OrderQty")).alias("TotalQuantitySold")
).select(
    F.col("ProductID"),
    F.col("Name").alias("ProductName"),
    F.col("TotalQuantitySold")
)

# 7. Identify slow-moving products (products with low sales and high inventory)
slow_moving_products = inventory_value.alias("iv").join(
    product_demand.alias("pd"),
    F.col("iv.ProductID") == F.col("pd.ProductID"),
    "inner"
).where(
    (F.col("pd.TotalQuantitySold") < 100) & (F.col("iv.TotalInventoryQuantity") > 500)
).select(
    F.col("iv.ProductID"),
    F.col("iv.ProductName"),
    F.col("iv.TotalInventoryQuantity"),
    F.col("pd.TotalQuantitySold")
)

# 8. Calculate safety stock levels (more advanced - requires historical data)
safety_stock = Production_Product.alias("p").join(
    product_demand,
    F.col("p.ProductID") == product_demand.ProductID,
    "inner"
).crossJoin(
    supplier_lead_time.agg(F.avg(F.col("AverageLeadTime")).alias("AvgLeadTime"))
).withColumn(
    "SafetyStockLevel",
    F.col("TotalQuantitySold") * 0.2 * (1 + (F.col("AvgLeadTime") / 30))
).select(
    F.col("p.ProductID"),
    F.col("p.Name").alias("ProductName"),
    F.col("SafetyStockLevel")
)

# 9. Analyze purchase order amounts by vendor
purchase_order_amounts = Purchasing_PurchaseOrderHeader.alias("po").join(
    Purchasing_Vendor.alias("v"),
    F.col("po.VendorID") == F.col("v.BusinessEntityID"),
    "inner"
).groupBy(
    F.col("po.VendorID"),
    F.col("v.Name")
).agg(
    F.sum(F.col("po.TotalDue")).alias("TotalPurchaseAmount")
).select(
    F.col("VendorID"),
    F.col("Name").alias("VendorName"),
    F.col("TotalPurchaseAmount")
)

# 10. Identify preferred vendors (top vendors by purchase amount)
preferred_vendors = purchase_order_amounts.withColumn(
    "VendorRank",
    F.rank().over(Window.orderBy(F.col("TotalPurchaseAmount").desc()))
).select(
    F.col("VendorID"),
    F.col("VendorName"),
    F.col("TotalPurchaseAmount"),
    F.col("VendorRank")
)

# 11. Calculate inventory turnover rate
inventory_turnover = inventory_value.alias("iv").join(
    product_demand.alias("pd"),
    F.col("iv.ProductID") == F.col("pd.ProductID"),
    "inner"
).join(
    Production_Product.select("ProductID", "StandardCost"),
    F.col("iv.ProductID") == F.col("ProductID"),
    "inner"
).withColumn(
    "TurnoverRate",
    F.when(F.col("iv.TotalInventoryValue") == 0, 0).otherwise(F.col("pd.TotalQuantitySold") * F.col("StandardCost") / F.col("iv.TotalInventoryValue"))
).select(
    F.col("iv.ProductID"),
    F.col("iv.ProductName"),
    F.col("pd.TotalQuantitySold"),
    F.col("iv.TotalInventoryValue"),
    F.col("TurnoverRate")
)

# 12. Forecast future demand (simplified moving average)
demand_forecast = product_demand_monthly.where(
    F.col("SalesYear") >= F.year(F.current_date()) - 1
).groupBy(
    F.col("ProductID"),
    F.col("ProductName")
).agg(
    F.avg(F.col("TotalQuantitySold")).alias("ForecastedDemand")
).select(
    F.col("ProductID"),
    F.col("ProductName"),
    F.col("ForecastedDemand")
)

# 13. Calculate Economic Order Quantity (EOQ) - Simplified
economic_order_quantity = Production_Product.alias("p").join(
    product_demand,
    F.col("p.ProductID") == product_demand.ProductID,
    "inner"
).withColumn(
    "EOQ",
    F.sqrt((2 * F.col("TotalQuantitySold") * 100) / (F.col("p.StandardCost") * 0.1))
).select(
    F.col("p.ProductID"),
    F.col("p.Name").alias("ProductName"),
    F.col("EOQ")
)

# 14. Analyze inventory holding costs (simplified)
inventory_holding_cost = inventory_value.withColumn(
    "HoldingCost",
    F.col("TotalInventoryValue") * 0.1
).select(
    F.col("ProductID"),
    F.col("ProductName"),
    F.col("HoldingCost")
)

# 15. Identify products with high holding costs and low turnover
high_cost_low_turnover = inventory_holding_cost.alias("ihc").join(
    inventory_turnover.alias("ito"),
    F.col("ihc.ProductID") == F.col("ito.ProductID"),
    "inner"
).where(
    (F.col("ihc.HoldingCost") > 1000) & (F.col("ito.TurnoverRate") < 1)
).select(
    F.col("ihc.ProductID"),
    F.col("ihc.ProductName"),
    F.col("ihc.HoldingCost"),
    F.col("ito.TurnoverRate")
)

# Final SELECT statement to combine and present the results
top_vendor = on_time_delivery_ranking.orderBy(F.col("OnTimeDeliveryPercentage").desc()).limit(1).select("VendorName", "OnTimeDeliveryPercentage")
preferred_vendor = preferred_vendors.orderBy(F.col("TotalPurchaseAmount").desc()).limit(1).select("VendorName")

final_result = inventory_value.alias("iv") \
    .join(low_stock_products.alias("lsp"), F.col("iv.ProductID") == F.col("lsp.ProductID"), "left") \
    .crossJoin(top_vendor) \
    .join(product_demand.alias("pd"), F.col("iv.ProductID") == F.col("pd.ProductID"), "left") \
    .join(slow_moving_products.alias("smp"), F.col("iv.ProductID") == F.col("smp.ProductID"), "left") \
    .join(safety_stock.alias("ss"), F.col("iv.ProductID") == F.col("ss.ProductID"), "left") \
    .crossJoin(preferred_vendor) \
    .join(inventory_turnover.alias("ito"), F.col("iv.ProductID") == F.col("ito.ProductID"), "left") \
    .join(demand_forecast.alias("df"), F.col("iv.ProductID") == F.col("df.ProductID"), "left") \
    .join(economic_order_quantity.alias("eoq"), F.col("iv.ProductID") == F.col("eoq.ProductID"), "left") \
    .join(inventory_holding_cost.alias("ihc"), F.col("iv.ProductID") == F.col("ihc.ProductID"), "left") \
    .join(high_cost_low_turnover.alias("hclt"), F.col("iv.ProductID") == F.col("hclt.ProductID"), "left") \
    .orderBy(F.col("iv.TotalInventoryValue").desc()) \
    .select(
        F.col("iv.ProductName"),
        F.col("iv.TotalInventoryValue"),
        F.col("lsp.ProductName").alias("LowStockProductName"),
        F.col("top_vendor.VendorName").alias("TopVendor"),
        F.col("top_vendor.OnTimeDeliveryPercentage"),
        F.col("pd.TotalQuantitySold"),
        F.col("smp.ProductName").alias("SlowMovingProductName"),
        F.col("ss.SafetyStockLevel"),
        F.col("preferred_vendor.VendorName").alias("PreferredVendor"),
        F.col("ito.TurnoverRate"),
        F.col("df.ForecastedDemand"),
        F.col("eoq.EOQ"),
        F.col("ihc.HoldingCost"),
        F.col("hclt.ProductName").alias("HighCostLowTurnoverProduct")
    )