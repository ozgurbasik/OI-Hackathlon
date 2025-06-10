"""
PySpark code transpiled from query_007.sql
Classification: Complex
Generated on: 2025-06-10 17:38:46
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# 1. Calculate monthly sales totals
monthly_sales = (
    sales_order_header
    .join(sales_order_detail, sales_order_header.SalesOrderID == sales_order_detail.SalesOrderID)
    .groupBy(F.year(sales_order_header.OrderDate).alias("SalesYear"), F.month(sales_order_header.OrderDate).alias("SalesMonth"))
    .agg(F.sum(sales_order_detail.LineTotal).alias("TotalSales"))
)

# 2. Calculate the average monthly sales
average_monthly_sales = monthly_sales.agg(F.avg("TotalSales").alias("AverageSales"))

# 3. Identify high-value customers (customers with total purchases above the average)
average_sales_value = average_monthly_sales.collect()[0]["AverageSales"]

high_value_customers = (
    sales_order_header
    .join(sales_order_detail, sales_order_header.SalesOrderID == sales_order_detail.SalesOrderID)
    .groupBy(sales_order_header.CustomerID)
    .agg(F.sum(sales_order_detail.LineTotal).alias("TotalPurchaseAmount"))
    .filter(F.col("TotalPurchaseAmount") > average_sales_value)
)

# 4. Rank products by sales volume
product_ranking = (
    product
    .join(sales_order_detail, product.ProductID == sales_order_detail.ProductID)
    .groupBy(product.ProductID, product.Name.alias("ProductName"))
    .agg(F.sum(sales_order_detail.LineTotal).alias("TotalSales"))
    .withColumn("SalesRank", F.rank().over(Window.orderBy(F.desc("TotalSales"))))
)

# 5. Calculate the percentage of sales by product category
total_sales_all_products = sales_order_detail.agg(F.sum("LineTotal")).collect()[0][0]

# Handle cases where total_sales_all_products is zero to prevent division by zero
total_sales_all_products = total_sales_all_products if total_sales_all_products else 1

category_sales_percentage = (
    product_category
    .join(product_subcategory, product_category.ProductCategoryID == product_subcategory.ProductCategoryID)
    .join(product, product_subcategory.ProductSubcategoryID == product.ProductSubcategoryID)
    .join(sales_order_detail, product.ProductID == sales_order_detail.ProductID)
    .groupBy(product_category.Name.alias("CategoryName"))
    .agg(F.sum(sales_order_detail.LineTotal).alias("CategorySales"))
    .withColumn(
        "SalesPercentage",
        F.col("CategorySales") * 100.0 / F.lit(total_sales_all_products)
    )
)

# 6. Determine the top selling product in each category
top_product_in_category = (
    product_category
    .join(product_subcategory, product_category.ProductCategoryID == product_subcategory.ProductCategoryID)
    .join(product, product_subcategory.ProductSubcategoryID == product.ProductSubcategoryID)
    .join(sales_order_detail, product.ProductID == sales_order_detail.ProductID)
    .groupBy(product_category.Name.alias("CategoryName"), product.Name.alias("ProductName"))
    .agg(F.sum(sales_order_detail.LineTotal).alias("TotalSales"))
    .withColumn(
        "RowNum",
        F.row_number().over(Window.partitionBy("CategoryName").orderBy(F.desc("TotalSales")))
    )
)

# 7. Calculate year-over-year sales growth
yearly_sales = (
    sales_order_header
    .join(sales_order_detail, sales_order_header.SalesOrderID == sales_order_detail.SalesOrderID)
    .groupBy(F.year(sales_order_header.OrderDate).alias("SalesYear"))
    .agg(F.sum(sales_order_detail.LineTotal).alias("TotalSales"))
    .orderBy("SalesYear")
)

year_over_year_growth = yearly_sales.withColumn(
    "PreviousYearSales",
    F.lag("TotalSales", 1, 0).over(Window.orderBy("SalesYear"))
).withColumn(
    "GrowthPercentage",
    F.when(
        F.col("PreviousYearSales") == 0,
        0
    ).otherwise(
        (F.col("TotalSales") - F.col("PreviousYearSales")) * 100.0 / F.col("PreviousYearSales")
    )
)

# 8. Analyze customer demographics (using dummy data since AdventureWorks lacks detailed demographics)
customer_demographics = (
    sales_customer
    .select("CustomerID")
    .limit(100)
    .orderBy("CustomerID")
    .withColumn(
        "Gender",
        F.when(F.col("CustomerID") % 2 == 0, "Male").otherwise("Female")
    )
    .withColumn(
        "AgeGroup",
        F.when(F.col("CustomerID") % 5 == 0, "25-34")
        .when(F.col("CustomerID") % 5 == 1, "35-44")
        .when(F.col("CustomerID") % 5 == 2, "45-54")
        .otherwise("55+")
    )
    .withColumn(
        "IncomeLevel",
        F.when(F.col("CustomerID") % 3 == 0, "High")
        .when(F.col("CustomerID") % 3 == 1, "Medium")
        .otherwise("Low")
    )
)

# 9. Combine customer demographics with purchase data
customer_purchase_analysis = (
    customer_demographics
    .join(sales_order_header, customer_demographics.CustomerID == sales_order_header.CustomerID)
    .groupBy(customer_demographics.Gender, customer_demographics.AgeGroup, customer_demographics.IncomeLevel)
    .agg(F.sum(sales_order_header.TotalDue).alias("TotalPurchases"))
)

# 10. Calculate shipping times
shipping_times = (
    sales_order_header
    .filter(sales_order_header.ShipDate.isNotNull())
    .withColumn("ShippingDays", F.datediff(sales_order_header.ShipDate, sales_order_header.OrderDate))
    .select("SalesOrderID", "OrderDate", "ShipDate", "ShippingDays")
)

# 11. Average shipping time by territory
avg_shipping_by_territory = (
    sales_sales_territory
    .join(sales_order_header, sales_sales_territory.TerritoryID == sales_order_header.TerritoryID)
    .join(shipping_times, sales_order_header.SalesOrderID == shipping_times.SalesOrderID)
    .groupBy(sales_sales_territory.Name.alias("TerritoryName"))
    .agg(F.avg(shipping_times.ShippingDays).alias("AvgShippingDays"))
)

# Get single row DataFrames by taking the first row
top_product_ranking = product_ranking.orderBy(F.desc("TotalSales")).limit(1).collect()[0]
top_category_sales_percentage = category_sales_percentage.orderBy(F.desc("SalesPercentage")).limit(1).collect()[0]
top_product_in_top_category = top_product_in_category.orderBy(F.desc("TotalSales")).limit(1).collect()[0]
top_customer_purchase_analysis = customer_purchase_analysis.orderBy(F.desc("TotalPurchases")).limit(1).collect()[0]
top_avg_shipping_by_territory = avg_shipping_by_territory.orderBy(F.asc("AvgShippingDays")).limit(1).collect()[0]


# Create a DataFrame by manually creating a single row with the data.
# Using lit to assign values and alias to match expected output.
final_result = (
    monthly_sales
    .crossJoin(average_monthly_sales)
    .withColumn("NumberOfHighValueCustomers", F.lit(high_value_customers.count()))
    .withColumn("TopSellingProduct", F.lit(top_product_ranking["ProductName"]))
    .withColumn("TopSellingProductSales", F.lit(top_product_ranking["TotalSales"]))
    .withColumn("HighestSalesCategory", F.lit(top_category_sales_percentage["CategoryName"]))
    .withColumn("HighestSalesCategoryPercentage", F.lit(top_category_sales_percentage["SalesPercentage"]))
    .withColumn("TopProductInTopCategory", F.lit(top_product_in_top_category["ProductName"]))
    .crossJoin(year_over_year_growth)
    .withColumn("CustomerGender", F.lit(top_customer_purchase_analysis["Gender"]))
    .withColumn("CustomerAgeGroup", F.lit(top_customer_purchase_analysis["AgeGroup"]))
    .withColumn("CustomerIncomeLevel", F.lit(top_customer_purchase_analysis["IncomeLevel"]))
    .withColumn("TotalCustomerPurchases", F.lit(top_customer_purchase_analysis["TotalPurchases"]))
    .withColumn("TerritoryName", F.lit(top_avg_shipping_by_territory["TerritoryName"]))
    .withColumn("AverageShippingDays", F.lit(top_avg_shipping_by_territory["AvgShippingDays"]))
    .select(
        F.col("SalesYear"),
        F.col("SalesMonth"),
        F.col("TotalSales").alias("MonthlySalesTotal"),
        "AverageSales as OverallAverageMonthlySales",
        "NumberOfHighValueCustomers",
        "TopSellingProduct",
        "TopSellingProductSales",
        "HighestSalesCategory",
        "HighestSalesCategoryPercentage",
        "TopProductInTopCategory",
        "GrowthPercentage as YearOverYearSalesGrowth",
        "CustomerGender",
        "CustomerAgeGroup",
        "CustomerIncomeLevel",
        "TotalCustomerPurchases",
        "TerritoryName",
        "AverageShippingDays"
    )
    .orderBy("SalesYear", "SalesMonth")
)