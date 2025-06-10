"""
PySpark code transpiled from query_008.sql
Classification: Complex
Generated on: 2025-06-10 17:38:32
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'

# 1. Calculate current inventory value for each product
inventory_value = (
    spark.table("Production.Product")
    .alias("p")
    .join(
        spark.table("Production.ProductInventory").alias("inv"),
        F.col("p.ProductID") == F.col("inv.ProductID"),
    )
    .groupBy("p.ProductID", "p.Name")
    .agg(
        F.sum(F.col("inv.Quantity") * F.col("p.StandardCost")).alias(
            "TotalInventoryValue"
        ),
        F.sum(F.col("inv.Quantity")).alias("TotalInventoryQuantity"),
    )
    .select(
        F.col("ProductID"),
        F.col("Name").alias("ProductName"),
        F.col("TotalInventoryValue"),
        F.col("TotalInventoryQuantity"),
    )
)

# 2. Identify products below reorder point
low_stock_products = (
    spark.table("Production.Product")
    .alias("p")
    .join(
        spark.table("Production.ProductInventory").alias("inv"),
        F.col("p.ProductID") == F.col("inv.ProductID"),
    )
    .groupBy("p.ProductID", "p.Name", "p.ReorderPoint")
    .agg(F.sum(F.col("inv.Quantity")).alias("CurrentQuantity"))
    .where(F.col("CurrentQuantity") < F.col("ReorderPoint"))
    .select(
        F.col("ProductID"),
        F.col("Name").alias("ProductName"),
        F.col("ReorderPoint"),
        F.col("CurrentQuantity"),
    )
)

# 3. Calculate average lead time from suppliers
supplier_lead_time = (
    spark.table("Purchasing.PurchaseOrderHeader")
    .alias("po")
    .join(
        spark.table("Purchasing.Vendor").alias("v"),
        F.col("po.VendorID") == F.col("v.BusinessEntityID"),
    )
    .where(F.col("po.ShipDate").isNotNull())
    .groupBy("po.VendorID", "v.Name")
    .agg(
        F.avg(F.datediff(F.col("po.ShipDate"), F.col("po.OrderDate"))).alias(
            "AverageLeadTime"
        )
    )
    .select(
        F.col("VendorID"), F.col("Name").alias("VendorName"), F.col("AverageLeadTime")
    )
)

# 4. Rank suppliers by on-time delivery performance
on_time_delivery_ranking = (
    spark.table("Purchasing.PurchaseOrderHeader")
    .alias("po")
    .join(
        spark.table("Purchasing.Vendor").alias("v"),
        F.col("po.VendorID") == F.col("v.BusinessEntityID"),
    )
    .where(F.col("po.ShipDate").isNotNull())
    .groupBy("po.VendorID", "v.Name")
    .agg(
        F.sum(
            F.when(F.col("po.ShipDate") <= F.col("po.ModifiedDate"), 1).otherwise(0)
        ).alias("OnTimeDeliveries"),
        F.count("*").alias("TotalDeliveries"),
    )
    .withColumn(
        "OnTimeDeliveryPercentage",
        F.when(
            F.col("TotalDeliveries") == 0, 0
        ).otherwise(  # Prevent division by zero
            (F.col("OnTimeDeliveries").cast("float") * 100) / F.col("TotalDeliveries")
        ),
    )
    .withColumn(
        "DeliveryRank",
        F.rank().over(
            Window.orderBy(
                F.when(F.col("TotalDeliveries") == 0, 0).otherwise(
                    (F.col("OnTimeDeliveries").cast("float") * 100)
                    / F.col("TotalDeliveries")
                ).desc()
            )
        ),
    )
    .select(
        F.col("VendorID"),
        F.col("Name").alias("VendorName"),
        F.col("OnTimeDeliveries"),
        F.col("TotalDeliveries"),
        F.col("OnTimeDeliveryPercentage"),
        F.col("DeliveryRank"),
    )
)


# 5. Calculate product demand based on sales history (monthly)
product_demand_monthly = (
    spark.table("Production.Product")
    .alias("p")
    .join(
        spark.table("Sales.SalesOrderDetail").alias("sod"),
        F.col("p.ProductID") == F.col("sod.ProductID"),
    )
    .join(
        spark.table("Sales.SalesOrderHeader").alias("soh"),
        F.col("sod.SalesOrderID") == F.col("soh.SalesOrderID"),
    )
    .groupBy(
        "p.ProductID",
        "p.Name",
        F.year("soh.OrderDate"),
        F.month("soh.OrderDate"),
    )
    .agg(F.sum(F.col("sod.OrderQty")).alias("TotalQuantitySold"))  # Corrected column name
    .select(
        F.col("ProductID"),
        F.col("Name").alias("ProductName"),
        F.col("year(OrderDate)").alias("SalesYear"),
        F.col("month(OrderDate)").alias("SalesMonth"),
        F.col("TotalQuantitySold"),
    )
)

# 6. Calculate product demand based on sales history (overall)
product_demand = (
    spark.table("Production.Product")
    .alias("p")
    .join(
        spark.table("Sales.SalesOrderDetail").alias("sod"),
        F.col("p.ProductID") == F.col("sod.ProductID"),
    )
    .groupBy("p.ProductID", "p.Name")
    .agg(F.sum(F.col("sod.OrderQty")).alias("TotalQuantitySold"))  # Corrected column name
    .select(
        F.col("ProductID"), F.col("Name").alias("ProductName"), F.col("TotalQuantitySold")
    )
)

# 7. Identify slow-moving products (products with low sales and high inventory)
slow_moving_products = (
    inventory_value.alias("iv")
    .join(product_demand.alias("pd"), F.col("iv.ProductID") == F.col("pd.ProductID"))
    .where((F.col("pd.TotalQuantitySold") < 100) & (F.col("iv.TotalInventoryQuantity") > 500))  # Example thresholds
    .select(
        F.col("iv.ProductID"),
        F.col("iv.ProductName"),
        F.col("iv.TotalInventoryQuantity"),
        F.col("pd.TotalQuantitySold"),
    )
)

# 8. Calculate safety stock levels (more advanced - requires historical data)
safety_stock = (
    spark.table("Production.Product")
    .alias("p")
    .join(product_demand, F.col("p.ProductID") == F.col("ProductDemand.ProductID"))
    .crossJoin(supplier_lead_time.select(F.avg("AverageLeadTime").alias("avg_lead_time_all")))
    .select(
        F.col("p.ProductID"),
        F.col("p.Name").alias("ProductName"),
        (F.col("ProductDemand.TotalQuantitySold") * 0.2 * (1 + (F.col("avg_lead_time_all") / 30))).alias("SafetyStockLevel"),
    )
)

# 9. Analyze purchase order amounts by vendor
purchase_order_amounts = (
    spark.table("Purchasing.PurchaseOrderHeader")
    .alias("po")
    .join(
        spark.table("Purchasing.Vendor").alias("v"),
        F.col("po.VendorID") == F.col("v.BusinessEntityID"),
    )
    .groupBy("po.VendorID", "v.Name")
    .agg(F.sum(F.col("po.TotalDue")).alias("TotalPurchaseAmount"))
    .select(
        F.col("VendorID"), F.col("Name").alias("VendorName"), F.col("TotalPurchaseAmount")
    )
)

# 10. Identify preferred vendors (top vendors by purchase amount)
preferred_vendors = (
    purchase_order_amounts.withColumn(
        "VendorRank", F.rank().over(Window.orderBy(F.col("TotalPurchaseAmount").desc()))
    )
    .select(
        F.col("VendorID"),
        F.col("VendorName"),
        F.col("TotalPurchaseAmount"),
        F.col("VendorRank"),
    )
)

# 11. Calculate inventory turnover rate
inventory_turnover = (
    inventory_value.alias("iv")
    .join(product_demand.alias("pd"), F.col("iv.ProductID") == F.col("pd.ProductID"))
    .crossJoin(spark.table("Production.Product").select(F.avg("StandardCost").alias("avg_standard_cost")))
    .selectExpr("iv.*", "pd.TotalQuantitySold", "avg_standard_cost")
    .withColumn(
        "TurnoverRate",
        F.when(F.col("TotalInventoryValue") == 0, 0).otherwise(
            F.col("TotalQuantitySold") * F.col("avg_standard_cost") / F.col("TotalInventoryValue")
        ),
    )
    .select(
        F.col("ProductID"),
        F.col("ProductName"),
        F.col("TotalQuantitySold"),
        F.col("TotalInventoryValue"),
        F.col("TurnoverRate"),
    )
)

# 12. Forecast future demand (simplified moving average)
current_year = F.year(F.current_date())
demand_forecast = (
    product_demand_monthly.where(F.col("SalesYear") >= current_year - 1)  # Use last year's data
    .groupBy("ProductID", "ProductName")
    .agg(F.avg("TotalQuantitySold").alias("ForecastedDemand"))
    .select(F.col("ProductID"), F.col("ProductName"), F.col("ForecastedDemand"))
)

# 13. Calculate Economic Order Quantity (EOQ) - Simplified
economic_order_quantity = (
    spark.table("Production.Product")
    .alias("p")
    .join(product_demand, F.col("p.ProductID") == F.col("ProductDemand.ProductID"))
    .select(
        F.col("p.ProductID"),
        F.col("p.Name").alias("ProductName"),
        F.sqrt(
            (2 * F.col("ProductDemand.TotalQuantitySold") * 100)
            / (F.col("p.StandardCost") * 0.1)
        ).alias("EOQ"),
    )
)

# 14. Analyze inventory holding costs (simplified)
inventory_holding_cost = inventory_value.select(
    F.col("ProductID"),
    F.col("ProductName"),
    (F.col("TotalInventoryValue") * 0.1).alias("HoldingCost"),
)

# 15. Identify products with high holding costs and low turnover
high_cost_low_turnover = (
    inventory_holding_cost.alias("ihc")
    .join(
        inventory_turnover.alias("ito"), F.col("ihc.ProductID") == F.col("ito.ProductID")
    )
    .where((F.col("ihc.HoldingCost") > 1000) & (F.col("ito.TurnoverRate") < 1))
    .select(
        F.col("ihc.ProductID"),
        F.col("ihc.ProductName"),
        F.col("ihc.HoldingCost"),
        F.col("ito.TurnoverRate"),
    )
)

# Final SELECT statement to combine and present the results
top_vendor = on_time_delivery_ranking.orderBy(F.desc("OnTimeDeliveryPercentage")).limit(1)
top_preferred_vendor = preferred_vendors.orderBy(F.desc("TotalPurchaseAmount")).limit(1)

final_result = (
    inventory_value.alias("iv")
    .join(
        low_stock_products.alias("lsp"),
        F.col("iv.ProductID") == F.col("lsp.ProductID"),
        "left",
    )
    .join(
        top_vendor.alias("otd"),
        F.lit(True),
        "left",  # Cross join using F.lit(True) as the join condition
    )
    .join(
        product_demand.alias("pd"),
        F.col("iv.ProductID") == F.col("pd.ProductID"),
        "left",
    )
    .join(
        slow_moving_products.alias("smp"),
        F.col("iv.ProductID") == F.col("smp.ProductID"),
        "left",
    )
    .join(
        safety_stock.alias("ss"),
        F.col("iv.ProductID") == F.col("ss.ProductID"),
        "left",
    )
    .join(
        top_preferred_vendor.alias("pv"),
        F.lit(True),
        "left",  # Cross join using F.lit(True) as the join condition
    )
    .join(
        inventory_turnover.alias("ito"),
        F.col("iv.ProductID") == F.col("ito.ProductID"),
        "left",
    )
    .join(
        demand_forecast.alias("df"),
        F.col("iv.ProductID") == F.col("df.ProductID"),
        "left",
    )
    .join(
        economic_order_quantity.alias("eoq"),
        F.col("iv.ProductID") == F.col("eoq.ProductID"),
        "left",
    )
    .join(
        inventory_holding_cost.alias("ihc"),
        F.col("iv.ProductID") == F.col("ihc.ProductID"),
        "left",
    )
    .join(
        high_cost_low_turnover.alias("hclt"),
        F.col("iv.ProductID") == F.col("hclt.ProductID"),
        "left",
    )
    .select(
        F.col("iv.ProductName"),
        F.col("iv.TotalInventoryValue"),
        F.col("lsp.ProductName").alias("LowStockProductName"),
        F.col("otd.VendorName").alias("TopVendor"),
        F.col("otd.OnTimeDeliveryPercentage"),
        F.col("pd.TotalQuantitySold"),
        F.col("smp.ProductName").alias("SlowMovingProductName"),
        F.col("ss.SafetyStockLevel"),
        F.col("pv.VendorName").alias("PreferredVendor"),
        F.col("ito.TurnoverRate"),
        F.col("df.ForecastedDemand"),
        F.col("eoq.EOQ"),
        F.col("ihc.HoldingCost"),
        F.col("hclt.ProductName").alias("HighCostLowTurnoverProduct"),
    )
    .orderBy(F.desc("TotalInventoryValue"))
)