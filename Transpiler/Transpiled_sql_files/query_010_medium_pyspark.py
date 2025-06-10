"""
PySpark code transpiled from query_010.sql
Classification: Medium
Generated on: 2025-06-10 19:28:14
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'

# Define paths to the Parquet files
base_path = "/mnt/advdata/AdventureWorks/"
products_path = base_path + "Products.parquet"
order_details_path = base_path + "OrderDetails.parquet"
orders_path = base_path + "Orders.parquet"
customers_path = base_path + "Customers.parquet"
categories_path = base_path + "Categories.parquet"
suppliers_path = base_path + "Suppliers.parquet"

# Read the data into Spark DataFrames
products_df = spark.read.parquet(products_path)
order_details_df = spark.read.parquet(order_details_path)
orders_df = spark.read.parquet(orders_path)
customers_df = spark.read.parquet(customers_path)
categories_df = spark.read.parquet(categories_path)
suppliers_df = spark.read.parquet(suppliers_path)

# Create the ProductSales CTE
product_sales_df = products_df.alias("p").join(
    order_details_df.alias("od"), F.col("p.ProductID") == F.col("od.ProductID")
).join(
    orders_df.alias("o"), F.col("od.OrderID") == F.col("o.OrderID")
).join(
    customers_df.alias("c"), F.col("o.CustomerID") == F.col("c.CustomerID")
).join(
    categories_df.alias("cat"), F.col("p.CategoryID") == F.col("cat.CategoryID")
).join(
    suppliers_df.alias("s"), F.col("p.SupplierID") == F.col("s.SupplierID")
).select(
    F.col("p.ProductID"),
    F.col("p.ProductName"),
    F.col("p.CategoryID"),
    F.col("p.UnitPrice"),
    F.col("p.UnitsInStock"),
    F.col("od.OrderID"),
    F.col("od.Quantity"),
    F.col("od.UnitPrice").alias("SalePrice"),
    F.col("od.Discount"),
    F.col("o.OrderDate"),
    F.col("o.CustomerID"),
    F.col("c.CompanyName"),
    F.col("c.Country"),
    F.col("cat.CategoryName"),
    F.col("s.CompanyName").alias("SupplierName"),
    (F.col("od.Quantity") * F.col("od.UnitPrice") * (1 - F.col("od.Discount"))).alias("NetSaleAmount")
)

# Create the ProductStats CTE
product_stats_df = product_sales_df.groupBy("ProductID").agg(
    F.sum("NetSaleAmount").alias("TotalRevenue"),
    F.countDistinct("OrderID").alias("TotalOrders"),
    F.countDistinct("CustomerID").alias("UniqueCustomers"),
    F.sum("Quantity").alias("TotalQuantitySold"),
    F.avg("NetSaleAmount").alias("AvgOrderValue")
).withColumn(
    "SalesCategory",
    F.when(F.col("TotalRevenue") >= 50000, "Top Performer")
    .when(F.col("TotalRevenue") >= 25000, "Strong Seller")
    .when(F.col("TotalRevenue") >= 10000, "Moderate Seller")
    .otherwise("Low Performer")
)

# Create the CategoryAverages CTE
category_averages_df = product_sales_df.groupBy("CategoryID", "CategoryName").agg(
    F.avg("NetSaleAmount").alias("AvgSalePerOrder"),
    F.avg("Quantity").alias("AvgQuantityPerOrder"),
    F.sum("NetSaleAmount").alias("TotalCategoryRevenue"),
    F.countDistinct("ProductID").alias("ProductsInCategory")
)

# Create the ProductCategoryStats CTE
product_category_stats_df = product_sales_df.groupBy("ProductID", "CategoryID", "CategoryName").agg(
    F.sum("NetSaleAmount").alias("ProductCategoryRevenue"),
    F.countDistinct("CustomerID").alias("CustomersInCategory"),
    F.avg("Discount").alias("AvgDiscountUsed")
)

# Final SELECT statement
final_df = products_df.alias("p").join(
    product_stats_df.alias("ps"), F.col("p.ProductID") == F.col("ps.ProductID")
).join(
    product_category_stats_df.alias("pcs"), F.col("p.ProductID") == F.col("pcs.ProductID")
).join(
    category_averages_df.alias("ca"), F.col("pcs.CategoryID") == F.col("ca.CategoryID")
).join(
    categories_df.alias("cat"), F.col("pcs.CategoryID") == F.col("cat.CategoryID")
).join(
    suppliers_df.alias("s"), F.col("p.SupplierID") == F.col("s.SupplierID")
).select(
    F.col("p.ProductName"),
    F.col("p.UnitPrice"),
    F.col("p.UnitsInStock"),
    F.col("ps.SalesCategory"),
    F.col("ps.TotalRevenue"),
    F.col("ps.TotalOrders"),
    F.col("ps.UniqueCustomers"),
    F.col("ps.TotalQuantitySold"),
    F.col("cat.CategoryName"),
    F.col("pcs.ProductCategoryRevenue"),
    F.col("ca.AvgSalePerOrder"),
    F.col("ca.TotalCategoryRevenue"),
    F.col("s.CompanyName").alias("SupplierName"),
    (F.round((F.col("ps.TotalQuantitySold") * 100.0 / F.nullif(F.col("p.UnitsInStock") + F.col("ps.TotalQuantitySold"), 0)), 2)).alias("StockTurnoverRate"),
    F.when(F.col("pcs.ProductCategoryRevenue") > F.col("ca.AvgSalePerOrder"), "Above Category Avg")
    .when(F.col("pcs.ProductCategoryRevenue") == F.col("ca.AvgSalePerOrder"), "At Category Avg")
    .otherwise("Below Category Avg").alias("CategoryPerformanceComparison"),
    F.when(F.col("ca.TotalCategoryRevenue") > 200000, "High Revenue Category")
    .when(F.col("ca.TotalCategoryRevenue") > 100000, "Medium Revenue Category")
    .otherwise("Low Revenue Category").alias("CategoryClassification"),
    F.when((F.col("ps.UniqueCustomers") > 50) & (F.col("ps.TotalRevenue") > 30000), "Market Leader")
    .when((F.col("ps.UniqueCustomers") > 25) & (F.col("ps.TotalRevenue") > 15000), "Strong Product")
    .when(F.col("ps.UniqueCustomers") > 10, "Growing Product")
    .otherwise("Niche Product").alias("MarketPosition"),
    F.when((F.col("p.UnitsInStock") < 10) & (F.col("ps.TotalQuantitySold") > 100), "Reorder Required")
    .when((F.col("p.UnitsInStock") < 25) & (F.col("ps.TotalQuantitySold") > 50), "Monitor Stock")
    .otherwise("Stock OK").alias("StockStatus")
).orderBy(F.col("ps.SalesCategory").desc(), F.col("ps.TotalRevenue").desc(), F.col("cat.CategoryName"))