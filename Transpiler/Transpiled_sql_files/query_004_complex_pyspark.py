"""
PySpark code transpiled from query_004.sql
Classification: Complex
Generated on: 2025-06-10 17:37:56
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'

# Define ProductMetrics CTE
product_metrics = (
    spark.table("Production.Product")
    .alias("p")
    .join(
        spark.table("Production.ProductSubcategory").alias("ps"),
        F.col("p.ProductSubcategoryID") == F.col("ps.ProductSubcategoryID"),
        "left",
    )
    .join(
        spark.table("Production.ProductCategory").alias("pc"),
        F.col("ps.ProductCategoryID") == F.col("pc.ProductCategoryID"),
        "left",
    )
    .select(
        F.col("p.ProductID"),
        F.col("p.Name").alias("ProductName"),
        F.col("p.ProductNumber"),
        F.col("ps.Name").alias("SubcategoryName"),
        F.col("pc.Name").alias("CategoryName"),
        F.col("p.ListPrice"),
        F.col("p.StandardCost"),
        (F.col("p.ListPrice") - F.col("p.StandardCost")).alias("GrossProfit"),
        F.when(F.col("p.ListPrice") > 0, F.round(((F.col("p.ListPrice") - F.col("p.StandardCost")) / F.col("p.ListPrice")) * 100, 2)).otherwise(0).alias("GrossProfitMargin"),
        F.col("p.SafetyStockLevel"),
        F.col("p.ReorderPoint"),
        F.col("p.DaysToManufacture"),
    )
    .where(F.col("ListPrice") > 0)
)

# Define SalesData CTE
sales_data = (
    spark.table("Sales.SalesOrderDetail")
    .alias("sod")
    .join(
        spark.table("Sales.SalesOrderHeader").alias("soh"),
        F.col("sod.SalesOrderID") == F.col("soh.SalesOrderID"),
        "inner",
    )
    .where(F.col("soh.OrderDate") >= "2012-01-01")
    .groupBy("sod.ProductID")
    .agg(
        F.countDistinct("sod.SalesOrderID").alias("OrderCount"),
        F.sum("sod.OrderQty").alias("TotalQuantitySold"),
        F.sum("sod.LineTotal").alias("TotalRevenue"),
        F.avg("sod.UnitPrice").alias("AvgSellingPrice"),
        F.max("soh.OrderDate").alias("LastSaleDate"),
        F.min("soh.OrderDate").alias("FirstSaleDate"),
        F.countDistinct(F.year("soh.OrderDate")).alias("SaleYears"),
        F.countDistinct("soh.CustomerID").alias("UniqueCustomers"),
        F.stddev("sod.OrderQty").alias("QtyStandardDeviation"),
    )
)

# Define InventoryData CTE
inventory_data = (
    spark.table("Production.ProductInventory")
    .alias("pi")
    .groupBy("pi.ProductID")
    .agg(
        F.sum("pi.Quantity").alias("TotalInventory"),
        F.count("pi.LocationID").alias("LocationCount"),
        F.avg("pi.Quantity").alias("AvgInventoryPerLocation"),
        F.max("pi.Quantity").alias("MaxLocationInventory"),
        F.min("pi.Quantity").alias("MinLocationInventory"),
    )
)

# Define CategoryAnalysis CTE
category_analysis = (
    product_metrics.alias("pm")
    .join(sales_data.alias("sd"), F.col("pm.ProductID") == F.col("sd.ProductID"), "left")
    .groupBy("pm.CategoryName")
    .agg(
        F.count("pm.ProductID").alias("ProductCount"),
        F.avg("pm.GrossProfitMargin").alias("AvgMargin"),
        F.sum("sd.TotalRevenue").alias("CategoryRevenue"),
        F.avg("sd.TotalQuantitySold").alias("AvgQuantityPerProduct"),
        F.max("sd.TotalRevenue").alias("TopProductRevenue"),
        F.min("sd.TotalRevenue").alias("LowestProductRevenue"),
        F.sum(F.when(F.col("sd.LastSaleDate") >= F.add_months(F.current_date(), -6), 1).otherwise(0)).alias("RecentlyActivProducts"),
    )
)

# Main Query
ranked_products = (
    product_metrics.alias("pm")
    .join(sales_data.alias("sd"), F.col("pm.ProductID") == F.col("sd.ProductID"), "left")
    .join(inventory_data.alias("id"), F.col("pm.ProductID") == F.col("id.ProductID"), "left")
    .join(category_analysis.alias("ca"), F.col("pm.CategoryName") == F.col("ca.CategoryName"), "left")
    .where(F.col("pm.CategoryName").isNotNull())
    .select(
        F.col("pm.CategoryName"),
        F.col("pm.SubcategoryName"),
        F.col("pm.ProductName"),
        F.col("pm.ProductNumber"),
        F.col("pm.ListPrice"),
        F.col("pm.StandardCost"),
        F.col("pm.GrossProfit"),
        F.col("pm.GrossProfitMargin"),
        F.col("sd.TotalQuantitySold"),
        F.col("sd.TotalRevenue"),
        F.col("sd.AvgSellingPrice"),
        F.col("sd.OrderCount"),
        F.col("sd.UniqueCustomers"),
        F.col("id.TotalInventory"),
        F.col("id.LocationCount"),
        (F.when((F.col("sd.TotalQuantitySold") > 0) & (F.col("id.TotalInventory") > 0), F.round(F.col("id.TotalInventory") / (F.col("sd.TotalQuantitySold") / 365.0), 1)).otherwise(None)).alias("DaysOfInventory"),
        F.when(F.col("sd.TotalQuantitySold").isNull(), "No Sales")
        .when(F.col("sd.LastSaleDate") < F.add_months(F.current_date(), -12), "Discontinued")
        .when(F.col("sd.TotalQuantitySold") < 100, "Low Volume")
        .when(F.col("sd.TotalQuantitySold") < 1000, "Medium Volume")
        .otherwise("High Volume")
        .alias("SalesCategory"),
        F.when(F.col("pm.GrossProfitMargin") > 50, "High Margin")
        .when(F.col("pm.GrossProfitMargin") > 30, "Good Margin")
        .when(F.col("pm.GrossProfitMargin") > 10, "Low Margin")
        .otherwise("Poor Margin")
        .alias("MarginCategory"),
        F.col("ca.CategoryRevenue"),
        F.col("ca.AvgMargin").alias("CategoryAvgMargin"),
        F.rank().over(Window.partitionBy("pm.CategoryName").orderBy(F.col("sd.TotalRevenue").desc())).alias("CategoryRank"),
        F.rank().over(Window.orderBy(F.col("sd.TotalRevenue").desc())).alias("OverallRank"),
        F.when(F.rank().over(Window.partitionBy("pm.CategoryName").orderBy(F.col("sd.TotalRevenue").desc())) <= 3, "Top 3 in Category")
        .when(F.rank().over(Window.partitionBy("pm.CategoryName").orderBy(F.col("sd.TotalRevenue").desc())) <= 10, "Top 10 in Category")
        .otherwise("Other")
        .alias("CategoryPosition"),
    )
)

# Subquery to get the top product in each category
top_products = (
    product_metrics.alias("pm2")
    .join(sales_data.alias("sd2"), F.col("pm2.ProductID") == F.col("sd2.ProductID"), "inner")
    .groupBy("pm2.CategoryName")
    .agg(F.first("pm2.ProductName", ignorenulls=True).alias("CategoryTopProduct"))
)

# Join with the subquery to get the CategoryTopProduct
final_result = (
    ranked_products.alias("rp")
    .join(top_products.alias("tp"), F.col("rp.CategoryName") == F.col("tp.CategoryName"), "left")
    .select(
        F.col("rp.*"),
        F.col("tp.CategoryTopProduct")
    )
    .orderBy("rp.CategoryName", F.col("sd.TotalRevenue").desc())
)

# final_result.show()