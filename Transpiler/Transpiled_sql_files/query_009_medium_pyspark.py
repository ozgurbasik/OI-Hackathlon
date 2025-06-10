"""
PySpark code transpiled from query_009.sql
Classification: Medium
Generated on: 2025-06-10 19:27:54
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'

# Define paths for input data
base_path = "/mnt/advdata/AdventureWorks/"
Customers_parquet = base_path + "Customers.parquet"
Orders_parquet = base_path + "Orders.parquet"
OrderDetails_parquet = base_path + "OrderDetails.parquet"
Products_parquet = base_path + "Products.parquet"
Categories_parquet = base_path + "Categories.parquet"

# Read data into DataFrames
customers_df = spark.read.parquet(Customers_parquet)
orders_df = spark.read.parquet(Orders_parquet)
order_details_df = spark.read.parquet(OrderDetails_parquet)
products_df = spark.read.parquet(Products_parquet)
categories_df = spark.read.parquet(Categories_parquet)

# Create CustomerOrders CTE
customer_orders_df = customers_df.alias("c").join(
    orders_df.alias("o"),
    F.col("c.CustomerID") == F.col("o.CustomerID"),
    "inner"
).join(
    order_details_df.alias("od"),
    F.col("o.OrderID") == F.col("od.OrderID"),
    "inner"
).join(
    products_df.alias("p"),
    F.col("od.ProductID") == F.col("p.ProductID"),
    "inner"
).join(
    categories_df.alias("cat"),
    F.col("p.CategoryID") == F.col("cat.CategoryID"),
    "inner"
).select(
    F.col("c.CustomerID"),
    F.col("c.FirstName"),
    F.col("c.LastName"),
    F.col("c.Email"),
    F.col("o.OrderID"),
    F.col("o.OrderDate"),
    F.col("od.ProductID"),
    F.col("od.Quantity"),
    F.col("od.UnitPrice"),
    F.col("p.ProductName"),
    F.col("p.CategoryID"),
    F.col("cat.CategoryName"),
    (F.col("od.Quantity") * F.col("od.UnitPrice")).alias("TotalAmount")
)

# Create CustomerStats CTE
customer_stats_df = customer_orders_df.groupBy("CustomerID").agg(
    F.sum("TotalAmount").alias("TotalSpent"),
    F.countDistinct("OrderID").alias("TotalOrders"),
    F.countDistinct("CategoryID").alias("DistinctCategories")
).withColumn(
    "LoyaltyTier",
    F.when(F.col("TotalSpent") >= 5000, "Platinum")
    .when(F.col("TotalSpent") >= 2000, "Gold")
    .when(F.col("TotalSpent") >= 1000, "Silver")
    .otherwise("Bronze")
)

# Create CategoryAverages CTE
category_averages_df = customer_orders_df.groupBy("CategoryID", "CategoryName").agg(
    F.avg("TotalAmount").alias("AvgSpendPerOrder"),
    F.sum("TotalAmount").alias("TotalCategoryRevenue")
)

# Create CustomerCategoryStats CTE
customer_category_stats_df = customer_orders_df.groupBy(
    "CustomerID", "CategoryID", "CategoryName"
).agg(
    F.sum("TotalAmount").alias("CustomerCategorySpend"),
    F.countDistinct("OrderID").alias("OrdersInCategory")
)

# Final SELECT statement
final_df = customers_df.alias("c").join(
    customer_stats_df.alias("cs"),
    F.col("c.CustomerID") == F.col("cs.CustomerID"),
    "inner"
).join(
    customer_category_stats_df.alias("ccs"),
    F.col("c.CustomerID") == F.col("ccs.CustomerID"),
    "inner"
).join(
    category_averages_df.alias("ca"),
    F.col("ccs.CategoryID") == F.col("ca.CategoryID"),
    "inner"
).join(
    categories_df.alias("cat"),
    F.col("ccs.CategoryID") == F.col("cat.CategoryID"),
    "inner"
).select(
    F.col("c.FirstName"),
    F.col("c.LastName"),
    F.col("c.Email"),
    F.col("cs.LoyaltyTier"),
    F.col("cs.TotalSpent"),
    F.col("cs.TotalOrders"),
    F.col("cs.DistinctCategories"),
    F.col("cat.CategoryName"),
    F.col("ccs.CustomerCategorySpend"),
    F.col("ca.AvgSpendPerOrder"),
    F.col("ca.TotalCategoryRevenue"),
    F.when(F.col("ccs.CustomerCategorySpend") > F.col("ca.AvgSpendPerOrder"), "Above Avg")
    .when(F.col("ccs.CustomerCategorySpend") == F.col("ca.AvgSpendPerOrder"), "Avg")
    .otherwise("Below Avg").alias("CategorySpendComparison"),
    F.when(F.col("ca.TotalCategoryRevenue") > 100000, "High Revenue Category")
    .when(F.col("ca.TotalCategoryRevenue") > 50000, "Medium Revenue Category")
    .otherwise("Low Revenue Category").alias("CategoryPerformance")
).orderBy(F.col("LoyaltyTier").desc(), F.col("TotalSpent").desc(), F.col("CategoryName"))