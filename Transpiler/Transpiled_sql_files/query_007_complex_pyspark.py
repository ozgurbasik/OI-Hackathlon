"""
PySpark code transpiled from query_007.sql
Classification: Complex
Generated on: 2025-06-10 19:29:25
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assume SparkSession is already created as 'spark'

# Define paths for parquet files
base_path = "/mnt/advdata/AdventureWorks/"
Sales_SalesOrderHeader_path = base_path + "Sales_SalesOrderHeader.parquet"
Sales_SalesOrderDetail_path = base_path + "Sales_SalesOrderDetail.parquet"
Production_Product_path = base_path + "Production_Product.parquet"
Production_ProductCategory_path = base_path + "Production_ProductCategory.parquet"
Production_ProductSubcategory_path = base_path + "Production_ProductSubcategory.parquet"
Sales_Customer_path = base_path + "Sales_Customer.parquet"
Sales_SalesTerritory_path = base_path + "Sales_SalesTerritory.parquet"

# Read data from parquet files into DataFrames
Sales_SalesOrderHeader = spark.read.parquet(Sales_SalesOrderHeader_path)
Sales_SalesOrderDetail = spark.read.parquet(Sales_SalesOrderDetail_path)
Production_Product = spark.read.parquet(Production_Product_path)
Production_ProductCategory = spark.read.parquet(Production_ProductCategory_path)
Production_ProductSubcategory = spark.read.parquet(Production_ProductSubcategory_path)
Sales_Customer = spark.read.parquet(Sales_Customer_path)
Sales_SalesTerritory = spark.read.parquet(Sales_SalesTerritory_path)

# 1. Calculate monthly sales totals
MonthlySales = (
    Sales_SalesOrderHeader.alias("soh")
    .join(
        Sales_SalesOrderDetail.alias("sod"),
        F.col("soh.SalesOrderID") == F.col("sod.SalesOrderID"),
    )
    .groupBy(F.year(F.col("soh.OrderDate")).alias("SalesYear"), F.month(F.col("soh.OrderDate")).alias("SalesMonth"))
    .agg(F.sum(F.col("sod.LineTotal")).alias("TotalSales"))
)

# 2. Calculate the average monthly sales
AverageMonthlySales = MonthlySales.agg(F.avg(F.col("TotalSales")).alias("AverageSales"))

# 3. Identify high-value customers (customers with total purchases above the average)
average_sales_value = AverageMonthlySales.collect()[0]["AverageSales"]
HighValueCustomers = (
    Sales_SalesOrderHeader.alias("soh")
    .join(
        Sales_SalesOrderDetail.alias("sod"),
        F.col("soh.SalesOrderID") == F.col("sod.SalesOrderID"),
    )
    .groupBy(F.col("soh.CustomerID"))
    .agg(F.sum(F.col("sod.LineTotal")).alias("TotalPurchaseAmount"))
    .filter(F.col("TotalPurchaseAmount") > average_sales_value)
)

# 4. Rank products by sales volume
ProductRanking = (
    Production_Product.alias("p")
    .join(
        Sales_SalesOrderDetail.alias("sod"),
        F.col("p.ProductID") == F.col("sod.ProductID"),
    )
    .groupBy(F.col("p.ProductID"), F.col("p.Name").alias("ProductName"))
    .agg(F.sum(F.col("sod.LineTotal")).alias("TotalSales"))
    .withColumn(
        "SalesRank",
        F.rank().over(Window.orderBy(F.col("TotalSales").desc())),
    )
)

# 5. Calculate the percentage of sales by product category
total_sales = Sales_SalesOrderDetail.agg(F.sum("LineTotal")).collect()[0][0]
CategorySalesPercentage = (
    Production_ProductCategory.alias("pc")
    .join(
        Production_ProductSubcategory.alias("psc"),
        F.col("pc.ProductCategoryID") == F.col("psc.ProductCategoryID"),
    )
    .join(
        Production_Product.alias("p"),
        F.col("psc.ProductSubcategoryID") == F.col("p.ProductSubcategoryID"),
    )
    .join(
        Sales_SalesOrderDetail.alias("sod"),
        F.col("p.ProductID") == F.col("sod.ProductID"),
    )
    .groupBy(F.col("pc.Name").alias("CategoryName"))
    .agg(F.sum(F.col("sod.LineTotal")).alias("CategorySales"))
    .withColumn(
        "SalesPercentage",
        F.when(F.lit(total_sales) == 0, F.lit(0.0)).otherwise(
            F.col("CategorySales") * 100.0 / F.lit(total_sales)
        ),
    )
)

# 6. Determine the top selling product in each category
TopProductInCategory = (
    Production_ProductCategory.alias("pc")
    .join(
        Production_ProductSubcategory.alias("psc"),
        F.col("pc.ProductCategoryID") == F.col("psc.ProductCategoryID"),
    )
    .join(
        Production_Product.alias("p"),
        F.col("psc.ProductSubcategoryID") == F.col("p.ProductSubcategoryID"),
    )
    .join(
        Sales_SalesOrderDetail.alias("sod"),
        F.col("p.ProductID") == F.col("sod.ProductID"),
    )
    .groupBy(F.col("pc.Name").alias("CategoryName"), F.col("p.Name").alias("ProductName"))
    .agg(F.sum(F.col("sod.LineTotal")).alias("TotalSales"))
    .withColumn(
        "RowNum",
        F.row_number().over(
            Window.partitionBy(F.col("CategoryName")).orderBy(F.col("TotalSales").desc())
        ),
    )
)

# 7. Calculate year-over-year sales growth
YearlySales = (
    Sales_SalesOrderHeader.alias("soh")
    .join(
        Sales_SalesOrderDetail.alias("sod"),
        F.col("soh.SalesOrderID") == F.col("sod.SalesOrderID"),
    )
    .groupBy(F.year(F.col("soh.OrderDate")).alias("SalesYear"))
    .agg(F.sum(F.col("sod.LineTotal")).alias("TotalSales"))
)

YearOverYearGrowth = YearlySales.withColumn(
    "PreviousYearSales",
    F.lag(F.col("TotalSales"), 1, 0).over(Window.orderBy(F.col("SalesYear"))),
).withColumn(
    "GrowthPercentage",
    F.when(
        F.col("PreviousYearSales") == 0,
        F.lit(0.0),
    ).otherwise(
        (F.col("TotalSales") - F.col("PreviousYearSales")) * 100.0 / F.col("PreviousYearSales")
    ),
)

# 8. Analyze customer demographics (using dummy data since AdventureWorks lacks detailed demographics)
CustomerDemographics = Sales_Customer.select(
    F.col("CustomerID"),
    F.when(F.col("CustomerID") % 2 == 0, "Male").otherwise("Female").alias("Gender"),
    F.when(F.col("CustomerID") % 5 == 0, "25-34")
    .when(F.col("CustomerID") % 5 == 1, "35-44")
    .when(F.col("CustomerID") % 5 == 2, "45-54")
    .otherwise("55+")
    .alias("AgeGroup"),
    F.when(F.col("CustomerID") % 3 == 0, "High")
    .when(F.col("CustomerID") % 3 == 1, "Medium")
    .otherwise("Low")
    .alias("IncomeLevel"),
).orderBy(F.col("CustomerID"))

# 9. Combine customer demographics with purchase data
CustomerPurchaseAnalysis = (
    CustomerDemographics.alias("cd")
    .join(
        Sales_SalesOrderHeader.alias("soh"),
        F.col("cd.CustomerID") == F.col("soh.CustomerID"),
    )
    .groupBy(F.col("cd.Gender"), F.col("cd.AgeGroup"), F.col("cd.IncomeLevel"))
    .agg(F.sum(F.col("soh.TotalDue")).alias("TotalPurchases"))
)

# 10. Calculate shipping times
ShippingTimes = Sales_SalesOrderHeader.filter(F.col("ShipDate").isNotNull()).select(
    F.col("SalesOrderID"),
    F.col("OrderDate"),
    F.col("ShipDate"),
    F.datediff(F.col("ShipDate"), F.col("OrderDate")).alias("ShippingDays"),
)

# 11. Average shipping time by territory
AvgShippingByTerritory = (
    Sales_SalesTerritory.alias("st")
    .join(
        Sales_SalesOrderHeader.alias("soh"),
        F.col("st.TerritoryID") == F.col("soh.TerritoryID"),
    )
    .join(
        ShippingTimes.alias("st2"),
        F.col("soh.SalesOrderID") == F.col("st2.SalesOrderID"),
    )
    .groupBy(F.col("st.Name").alias("TerritoryName"))
    .agg(F.avg(F.col("st2.ShippingDays")).alias("AvgShippingDays"))
)

# Find top 1 from ProductRanking
TopProductRanking = ProductRanking.orderBy(F.desc("TotalSales")).limit(1).withColumnRenamed("ProductName", "TopSellingProduct").withColumnRenamed("TotalSales", "TopSellingProductSales")

# Find top 1 from CategorySalesPercentage
TopCategorySalesPercentage = CategorySalesPercentage.orderBy(F.desc("SalesPercentage")).limit(1).withColumnRenamed("CategoryName", "HighestSalesCategory").withColumnRenamed("SalesPercentage", "HighestSalesCategoryPercentage")

# Find top 1 from TopProductInCategory
TopTopProductInCategory = TopProductInCategory.orderBy(F.desc("TotalSales")).limit(1).withColumnRenamed("ProductName", "TopProductInTopCategory").select("CategoryName", "TopProductInTopCategory")

# Find top 1 from CustomerPurchaseAnalysis
TopCustomerPurchaseAnalysis = CustomerPurchaseAnalysis.orderBy(F.desc("TotalPurchases")).limit(1).withColumnRenamed("Gender", "CustomerGender").withColumnRenamed("AgeGroup", "CustomerAgeGroup").withColumnRenamed("IncomeLevel", "CustomerIncomeLevel").withColumnRenamed("TotalPurchases", "TotalCustomerPurchases")

# Find top 1 from AvgShippingByTerritory
TopAvgShippingByTerritory = AvgShippingByTerritory.orderBy(F.asc("AvgShippingDays")).limit(1).withColumnRenamed("TerritoryName", "TerritoryName").withColumnRenamed("AvgShippingDays", "AverageShippingDays")

# Get count from HighValueCustomers
NumberOfHighValueCustomers = HighValueCustomers.count()
NumberOfHighValueCustomers_df = spark.createDataFrame([(NumberOfHighValueCustomers,)], ['NumberOfHighValueCustomers'])

# Join all the tables

Final_Result = (
    MonthlySales.alias("ms")
    .crossJoin(AverageMonthlySales.alias("ams"))
    .crossJoin(TopProductRanking.alias("pr"))
    .crossJoin(TopCategorySalesPercentage.alias("csp"))
    .crossJoin(TopTopProductInCategory.alias("tpic"))
    .crossJoin(YearOverYearGrowth.alias("yy"))
    .crossJoin(TopCustomerPurchaseAnalysis.alias("cpa"))
    .crossJoin(TopAvgShippingByTerritory.alias("ast"))
    .crossJoin(NumberOfHighValueCustomers_df.alias("num"))
    .select(
        "ms.SalesYear",
        "ms.SalesMonth",
        F.col("ms.TotalSales").alias("MonthlySalesTotal"),
        F.col("ams.AverageSales").alias("OverallAverageMonthlySales"),
        F.col("num.NumberOfHighValueCustomers"),
        "pr.TopSellingProduct",
        F.col("pr.TopSellingProductSales"),
        "csp.HighestSalesCategory",
        F.col("csp.HighestSalesCategoryPercentage"),
        "tpic.TopProductInTopCategory",
        "yy.GrowthPercentage",
        "cpa.CustomerGender",
        "cpa.CustomerAgeGroup",
        "cpa.CustomerIncomeLevel",
        F.col("cpa.TotalCustomerPurchases"),
        "ast.TerritoryName",
        "ast.AverageShippingDays",
    ).orderBy(["SalesYear","SalesMonth"])
)