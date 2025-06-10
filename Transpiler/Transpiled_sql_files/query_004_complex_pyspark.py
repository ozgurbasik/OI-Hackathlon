"""
PySpark code transpiled from query_004.sql
Classification: Complex
Generated on: 2025-06-10 19:28:28
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Define paths for the input parquet files
base_path = "/mnt/advdata/AdventureWorks/"
Production_Product_path = base_path + "Production_Product.parquet"
Production_ProductSubcategory_path = base_path + "Production_ProductSubcategory.parquet"
Production_ProductCategory_path = base_path + "Production_ProductCategory.parquet"
Sales_SalesOrderDetail_path = base_path + "Sales_SalesOrderDetail.parquet"
Sales_SalesOrderHeader_path = base_path + "Sales_SalesOrderHeader.parquet"
Production_ProductInventory_path = base_path + "Production_ProductInventory.parquet"

# Read the data into Spark DataFrames
Production_Product = spark.read.parquet(Production_Product_path)
Production_ProductSubcategory = spark.read.parquet(Production_ProductSubcategory_path)
Production_ProductCategory = spark.read.parquet(Production_ProductCategory_path)
Sales_SalesOrderDetail = spark.read.parquet(Sales_SalesOrderDetail_path)
Sales_SalesOrderHeader = spark.read.parquet(Sales_SalesOrderHeader_path)
Production_ProductInventory = spark.read.parquet(Production_ProductInventory_path)

# Common Table Expression (CTE) - ProductMetrics
ProductMetrics = Production_Product.alias("p").join(
    Production_ProductSubcategory.alias("ps"),
    F.col("p.ProductSubcategoryID") == F.col("ps.ProductSubcategoryID"),
    "left"
).join(
    Production_ProductCategory.alias("pc"),
    F.col("ps.ProductCategoryID") == F.col("pc.ProductCategoryID"),
    "left"
).select(
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
    F.col("p.DaysToManufacture")
).where(F.col("p.ListPrice") > 0)

# CTE - SalesData
SalesData = Sales_SalesOrderDetail.alias("sod").join(
    Sales_SalesOrderHeader.alias("soh"),
    F.col("sod.SalesOrderID") == F.col("soh.SalesOrderID"),
    "inner"
).where(F.col("soh.OrderDate") >= '2012-01-01').groupBy(F.col("sod.ProductID")).agg(
    F.countDistinct(F.col("sod.SalesOrderID")).alias("OrderCount"),
    F.sum(F.col("sod.OrderQty")).alias("TotalQuantitySold"),
    F.sum(F.col("sod.LineTotal")).alias("TotalRevenue"),
    F.avg(F.col("sod.UnitPrice")).alias("AvgSellingPrice"),
    F.max(F.col("soh.OrderDate")).alias("LastSaleDate"),
    F.min(F.col("soh.OrderDate")).alias("FirstSaleDate"),
    F.countDistinct(F.year(F.col("soh.OrderDate"))).alias("SaleYears"),
    F.countDistinct(F.col("soh.CustomerID")).alias("UniqueCustomers"),
    F.stddev(F.col("sod.OrderQty")).alias("QtyStandardDeviation")
)

# CTE - InventoryData
InventoryData = Production_ProductInventory.alias("pi").groupBy(F.col("pi.ProductID")).agg(
    F.sum(F.col("pi.Quantity")).alias("TotalInventory"),
    F.count(F.col("pi.LocationID")).alias("LocationCount"),
    F.avg(F.col("pi.Quantity")).alias("AvgInventoryPerLocation"),
    F.max(F.col("pi.Quantity")).alias("MaxLocationInventory"),
    F.min(F.col("pi.Quantity")).alias("MinLocationInventory")
)

# CTE - CategoryAnalysis
CategoryAnalysis = ProductMetrics.alias("pm").join(
    SalesData.alias("sd"),
    F.col("pm.ProductID") == F.col("sd.ProductID"),
    "left"
).groupBy(F.col("pm.CategoryName")).agg(
    F.count(F.col("pm.ProductID")).alias("ProductCount"),
    F.avg(F.col("pm.GrossProfitMargin")).alias("AvgMargin"),
    F.sum(F.col("sd.TotalRevenue")).alias("CategoryRevenue"),
    F.avg(F.col("sd.TotalQuantitySold")).alias("AvgQuantityPerProduct"),
    F.max(F.col("sd.TotalRevenue")).alias("TopProductRevenue"),
    F.min(F.col("sd.TotalRevenue")).alias("LowestProductRevenue"),
    F.count(F.when(F.col("sd.LastSaleDate") >= F.add_months(F.current_date(), -6), 1)).alias("RecentlyActivProducts")
)

# Main Query
ranked_products = ProductMetrics.alias("pm").join(
    SalesData.alias("sd"),
    F.col("pm.ProductID") == F.col("sd.ProductID"),
    "left"
).join(
    InventoryData.alias("id"),
    F.col("pm.ProductID") == F.col("id.ProductID"),
    "left"
).join(
    CategoryAnalysis.alias("ca"),
    F.col("pm.CategoryName") == F.col("ca.CategoryName"),
    "left"
).where(F.col("pm.CategoryName").isNotNull()).select(
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
    F.when((F.col("sd.TotalQuantitySold") > 0) & (F.col("id.TotalInventory") > 0),
           F.round(F.col("id.TotalInventory") / (F.col("sd.TotalQuantitySold") / 365.0), 1)).otherwise(None).alias("DaysOfInventory"),
    F.when(F.col("sd.TotalQuantitySold").isNull(), "No Sales")
     .when(F.col("sd.LastSaleDate") < F.add_months(F.current_date(), -12), "Discontinued")
     .when(F.col("sd.TotalQuantitySold") < 100, "Low Volume")
     .when(F.col("sd.TotalQuantitySold") < 1000, "Medium Volume")
     .otherwise("High Volume").alias("SalesCategory"),
    F.when(F.col("pm.GrossProfitMargin") > 50, "High Margin")
     .when(F.col("pm.GrossProfitMargin") > 30, "Good Margin")
     .when(F.col("pm.GrossProfitMargin") > 10, "Low Margin")
     .otherwise("Poor Margin").alias("MarginCategory"),
    F.col("ca.CategoryRevenue"),
    F.col("ca.AvgMargin").alias("CategoryAvgMargin")
)

# Window Specifications for Ranking
category_window = Window.partitionBy("CategoryName").orderBy(F.desc("TotalRevenue"))
overall_window = Window.orderBy(F.desc("TotalRevenue"))

ranked_products = ranked_products.withColumn("CategoryRank", F.rank().over(category_window))
ranked_products = ranked_products.withColumn("OverallRank", F.rank().over(overall_window))

ranked_products = ranked_products.withColumn(
    "CategoryPosition",
    F.when(F.col("CategoryRank") <= 3, "Top 3 in Category")
     .when(F.col("CategoryRank") <= 10, "Top 10 in Category")
     .otherwise("Other")
)

# Subquery to get CategoryTopProduct
top_products_per_category = ProductMetrics.alias("pm2").join(
    SalesData.alias("sd2"),
    F.col("pm2.ProductID") == F.col("sd2.ProductID")
).groupBy(F.col("pm2.CategoryName")).agg(
    F.first(F.col("pm2.ProductName"), ignorenulls=True).alias("CategoryTopProduct")
)

# Joining the main query with the CategoryTopProduct subquery result
final_result = ranked_products.join(
    top_products_per_category,
    ranked_products["CategoryName"] == top_products_per_category["CategoryName"],
    "left"
).select(
    ranked_products["*"],
    top_products_per_category["CategoryTopProduct"]
).orderBy(F.col("CategoryName"), F.desc("TotalRevenue"))