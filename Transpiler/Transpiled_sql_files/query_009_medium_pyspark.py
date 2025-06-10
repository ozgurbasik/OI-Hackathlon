"""
PySpark code transpiled from query_009.sql
Classification: Medium
Generated on: 2025-06-10 17:37:23
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'

# Create dummy dataframes (replace with your actual data loading)
customers_df = spark.createDataFrame([(1, 'John', 'Doe', 'john.doe@example.com'), (2, 'Jane', 'Smith', 'jane.smith@example.com')], ['CustomerID', 'FirstName', 'LastName', 'Email'])
orders_df = spark.createDataFrame([(101, 1, '2023-01-15'), (102, 2, '2023-02-20')], ['OrderID', 'CustomerID', 'OrderDate'])
order_details_df = spark.createDataFrame([(101, 1, 2, 10, 2.50), (102, 2, 5, 5, 10.00)], ['OrderID', 'ProductID', 'Quantity', 'UnitPrice', 'Discount'])
products_df = spark.createDataFrame([(1, 'Laptop', 1), (2, 'Keyboard', 1), (5, 'Shirt', 2)], ['ProductID', 'ProductName', 'CategoryID'])
categories_df = spark.createDataFrame([(1, 'Electronics'), (2, 'Clothing')], ['CategoryID', 'CategoryName'])

# CustomerOrders CTE
customer_orders_df = customers_df.alias("c").join(orders_df.alias("o"), F.col("c.CustomerID") == F.col("o.CustomerID")) \
    .join(order_details_df.alias("od"), F.col("o.OrderID") == F.col("od.OrderID")) \
    .join(products_df.alias("p"), F.col("od.ProductID") == F.col("p.ProductID")) \
    .join(categories_df.alias("cat"), F.col("p.CategoryID") == F.col("cat.CategoryID")) \
    .select(
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

# CustomerStats CTE
customer_stats_df = customer_orders_df.groupBy("CustomerID") \
    .agg(
        F.sum("TotalAmount").alias("TotalSpent"),
        F.countDistinct("OrderID").alias("TotalOrders"),
        F.countDistinct("CategoryID").alias("DistinctCategories")
    ) \
    .withColumn(
        "LoyaltyTier",
        F.when(F.col("TotalSpent") >= 5000, "Platinum")
        .when(F.col("TotalSpent") >= 2000, "Gold")
        .when(F.col("TotalSpent") >= 1000, "Silver")
        .otherwise("Bronze")
    )

# CategoryAverages CTE
category_averages_df = customer_orders_df.groupBy("CategoryID", "CategoryName") \
    .agg(
        F.avg("TotalAmount").alias("AvgSpendPerOrder"),
        F.sum("TotalAmount").alias("TotalCategoryRevenue")
    )

# CustomerCategoryStats CTE
customer_category_stats_df = customer_orders_df.groupBy("CustomerID", "CategoryID", "CategoryName") \
    .agg(
        F.sum("TotalAmount").alias("CustomerCategorySpend"),
        F.countDistinct("OrderID").alias("OrdersInCategory")
    )

# Final SELECT statement
final_df = customers_df.alias("c").join(customer_stats_df.alias("cs"), F.col("c.CustomerID") == F.col("cs.CustomerID")) \
    .join(customer_category_stats_df.alias("ccs"), F.col("c.CustomerID") == F.col("ccs.CustomerID")) \
    .join(category_averages_df.alias("ca"), F.col("ccs.CategoryID") == F.col("ca.CategoryID")) \
    .join(categories_df.alias("cat"), F.col("ccs.CategoryID") == F.col("cat.CategoryID")) \
    .select(
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
    ) \
    .orderBy(F.col("LoyaltyTier").desc(), F.col("TotalSpent").desc(), F.col("CategoryName"))