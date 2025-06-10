"""
PySpark code transpiled from query_010.sql
Classification: Medium
Generated on: 2025-06-10 17:37:44
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'

# Define the ProductSales CTE
product_sales = (
    products_df
    .join(order_details_df, products_df["ProductID"] == order_details_df["ProductID"], "inner")
    .join(orders_df, order_details_df["OrderID"] == orders_df["OrderID"], "inner")
    .join(customers_df, orders_df["CustomerID"] == customers_df["CustomerID"], "inner")
    .join(categories_df, products_df["CategoryID"] == categories_df["CategoryID"], "inner")
    .join(suppliers_df, products_df["SupplierID"] == suppliers_df["SupplierID"], "inner")
    .select(
        products_df["ProductID"],
        products_df["ProductName"],
        products_df["CategoryID"],
        products_df["UnitPrice"],
        products_df["UnitsInStock"],
        order_details_df["OrderID"],
        order_details_df["Quantity"],
        order_details_df["UnitPrice"].alias("SalePrice"),
        order_details_df["Discount"],
        orders_df["OrderDate"],
        orders_df["CustomerID"],
        customers_df["CompanyName"],
        customers_df["Country"],
        categories_df["CategoryName"],
        suppliers_df["CompanyName"].alias("SupplierName"),
        (order_details_df["Quantity"] * order_details_df["UnitPrice"] * (1 - order_details_df["Discount"])).alias("NetSaleAmount")
    )
)

# Define the ProductStats CTE
product_stats = (
    product_sales
    .groupBy("ProductID")
    .agg(
        F.sum("NetSaleAmount").alias("TotalRevenue"),
        F.countDistinct("OrderID").alias("TotalOrders"),
        F.countDistinct("CustomerID").alias("UniqueCustomers"),
        F.sum("Quantity").alias("TotalQuantitySold"),
        F.avg("NetSaleAmount").alias("AvgOrderValue")
    )
    .withColumn(
        "SalesCategory",
        F.when(F.col("TotalRevenue") >= 50000, "Top Performer")
        .when(F.col("TotalRevenue") >= 25000, "Strong Seller")
        .when(F.col("TotalRevenue") >= 10000, "Moderate Seller")
        .otherwise("Low Performer")
    )
)

# Define the CategoryAverages CTE
category_averages = (
    product_sales
    .groupBy("CategoryID", "CategoryName")
    .agg(
        F.avg("NetSaleAmount").alias("AvgSalePerOrder"),
        F.avg("Quantity").alias("AvgQuantityPerOrder"),
        F.sum("NetSaleAmount").alias("TotalCategoryRevenue"),
        F.countDistinct("ProductID").alias("ProductsInCategory")
    )
)

# Define the ProductCategoryStats CTE
product_category_stats = (
    product_sales
    .groupBy("ProductID", "CategoryID", "CategoryName")
    .agg(
        F.sum("NetSaleAmount").alias("ProductCategoryRevenue"),
        F.countDistinct("CustomerID").alias("CustomersInCategory"),
        F.avg("Discount").alias("AvgDiscountUsed")
    )
)

# Final SELECT statement with JOINs and CASE statements
final_result = (
    products_df
    .join(product_stats, products_df["ProductID"] == product_stats["ProductID"], "inner")
    .join(product_category_stats, products_df["ProductID"] == product_category_stats["ProductID"], "inner")
    .join(category_averages, product_category_stats["CategoryID"] == category_averages["CategoryID"], "inner")
    .join(categories_df, product_category_stats["CategoryID"] == categories_df["CategoryID"], "inner")
    .join(suppliers_df, products_df["SupplierID"] == suppliers_df["SupplierID"], "inner")
    .select(
        products_df["ProductName"],
        products_df["UnitPrice"],
        products_df["UnitsInStock"],
        product_stats["SalesCategory"],
        product_stats["TotalRevenue"],
        product_stats["TotalOrders"],
        product_stats["UniqueCustomers"],
        product_stats["TotalQuantitySold"],
        categories_df["CategoryName"],
        product_category_stats["ProductCategoryRevenue"],
        category_averages["AvgSalePerOrder"],
        category_averages["TotalCategoryRevenue"],
        suppliers_df["CompanyName"].alias("SupplierName"),
        F.round((product_stats["TotalQuantitySold"] * 100.0 / F.nullif(products_df["UnitsInStock"] + product_stats["TotalQuantitySold"], 0)), 2).alias("StockTurnoverRate"),
        F.when(product_category_stats["ProductCategoryRevenue"] > category_averages["AvgSalePerOrder"], "Above Category Avg")
        .when(product_category_stats["ProductCategoryRevenue"] == category_averages["AvgSalePerOrder"], "At Category Avg")
        .otherwise("Below Category Avg").alias("CategoryPerformanceComparison"),
        F.when(category_averages["TotalCategoryRevenue"] > 200000, "High Revenue Category")
        .when(category_averages["TotalCategoryRevenue"] > 100000, "Medium Revenue Category")
        .otherwise("Low Revenue Category").alias("CategoryClassification"),
        F.when((product_stats["UniqueCustomers"] > 50) & (product_stats["TotalRevenue"] > 30000), "Market Leader")
        .when((product_stats["UniqueCustomers"] > 25) & (product_stats["TotalRevenue"] > 15000), "Strong Product")
        .when(product_stats["UniqueCustomers"] > 10, "Growing Product")
        .otherwise("Niche Product").alias("MarketPosition"),
        F.when((products_df["UnitsInStock"] < 10) & (product_stats["TotalQuantitySold"] > 100), "Reorder Required")
        .when((products_df["UnitsInStock"] < 25) & (product_stats["TotalQuantitySold"] > 50), "Monitor Stock")
        .otherwise("Stock OK").alias("StockStatus")
    )
    .orderBy(F.col("SalesCategory").desc(), F.col("TotalRevenue").desc(), F.col("CategoryName"))
)