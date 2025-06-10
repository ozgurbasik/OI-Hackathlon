"""
PySpark code transpiled from query_006.sql
Classification: Complex
Generated on: 2025-06-10 19:28:47
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'

# Define paths for the parquet files
base_path = "/mnt/advdata/AdventureWorks/"

# Read data from parquet files
HumanResources_Employee = spark.read.parquet(base_path + "HumanResources_Employee.parquet")
Person_Person = spark.read.parquet(base_path + "Person_Person.parquet")
HumanResources_EmployeeDepartmentHistory = spark.read.parquet(base_path + "HumanResources_EmployeeDepartmentHistory.parquet")
HumanResources_Department = spark.read.parquet(base_path + "HumanResources_Department.parquet")
Production_Product = spark.read.parquet(base_path + "Production_Product.parquet")
Production_ProductSubcategory = spark.read.parquet(base_path + "Production_ProductSubcategory.parquet")
Production_ProductCategory = spark.read.parquet(base_path + "Production_ProductCategory.parquet")
Sales_SalesOrderDetail = spark.read.parquet(base_path + "Sales_SalesOrderDetail.parquet")
Sales_SalesOrderHeader = spark.read.parquet(base_path + "Sales_SalesOrderHeader.parquet")
Production_ProductInventory = spark.read.parquet(base_path + "Production_ProductInventory.parquet")


# CTE: EmployeeDetails
employee_details = HumanResources_Employee.alias("e") \
    .join(Person_Person.alias("p"), F.col("e.BusinessEntityID") == F.col("p.BusinessEntityID")) \
    .join(HumanResources_EmployeeDepartmentHistory.alias("dh"), F.col("e.BusinessEntityID") == F.col("dh.BusinessEntityID")) \
    .join(HumanResources_Department.alias("d"), F.col("dh.DepartmentID") == F.col("d.DepartmentID")) \
    .where((F.col("e.CurrentFlag") == True) & (F.col("dh.EndDate").isNull())) \
    .select(
        F.col("e.BusinessEntityID"),
        F.col("p.FirstName"),
        F.col("p.LastName"),
        F.col("e.JobTitle"),
        F.col("e.HireDate"),
        F.col("e.VacationHours"),
        F.col("e.SickLeaveHours"),
        F.col("e.SalariedFlag"),
        F.col("dh.DepartmentID"),
        F.col("d.Name").alias("DepartmentName"),
        F.when(F.col("e.SalariedFlag") == 1, "Salaried").otherwise("Hourly").alias("PayType"),
        F.when(F.year(F.current_date()) - F.year(F.col("e.HireDate")) >= 10, "Senior")
        .when(F.year(F.current_date()) - F.year(F.col("e.HireDate")) >= 5, "Mid-Level")
        .otherwise("Junior").alias("ExperienceLevel")
    )

# CTE: DepartmentStats
department_stats = employee_details.groupBy("DepartmentName") \
    .agg(
        F.count("*").alias("TotalEmployees"),
        F.sum(F.when(F.col("PayType") == "Salaried", 1).otherwise(0)).alias("SalariedEmployees"),
        F.sum(F.when(F.col("PayType") == "Hourly", 1).otherwise(0)).alias("HourlyEmployees"),
        F.sum(F.when(F.col("ExperienceLevel") == "Senior", 1).otherwise(0)).alias("SeniorEmployees"),
        F.sum(F.when(F.col("ExperienceLevel") == "Mid-Level", 1).otherwise(0)).alias("MidLevelEmployees"),
        F.sum(F.when(F.col("ExperienceLevel") == "Junior", 1).otherwise(0)).alias("JuniorEmployees"),
        F.avg("VacationHours").alias("AvgVacationHours"),
        F.avg("SickLeaveHours").alias("AvgSickLeaveHours"),
        F.max("VacationHours").alias("MaxVacationHours"),
        F.min("VacationHours").alias("MinVacationHours")
    )

# CTE: PayTypeBreakdown
pay_type_breakdown = employee_details.groupBy("DepartmentName", "PayType") \
    .agg(
        F.count("*").alias("CountByPayType"),
        F.avg("VacationHours").alias("AvgVacationByPayType"),
        F.avg("SickLeaveHours").alias("AvgSickLeaveByPayType")
    )

# Final SELECT statement
final_result = employee_details.alias("ed") \
    .join(department_stats.alias("ds"), F.col("ed.DepartmentName") == F.col("ds.DepartmentName")) \
    .join(pay_type_breakdown.alias("ptb"), (F.col("ed.DepartmentName") == F.col("ptb.DepartmentName")) & (F.col("ed.PayType") == F.col("ptb.PayType"))) \
    .select(
        F.col("ed.DepartmentName"),
        F.col("ed.FirstName"),
        F.col("ed.LastName"),
        F.col("ed.JobTitle"),
        F.col("ed.HireDate"),
        F.col("ed.PayType"),
        F.col("ed.ExperienceLevel"),
        F.col("ed.VacationHours"),
        F.col("ed.SickLeaveHours"),
        F.col("ds.TotalEmployees"),
        F.col("ds.SalariedEmployees"),
        F.col("ds.HourlyEmployees"),
        F.col("ds.SeniorEmployees"),
        F.col("ds.MidLevelEmployees"),
        F.col("ds.JuniorEmployees"),
        F.round(F.col("ds.AvgVacationHours"), 1).alias("DeptAvgVacation"),
        F.round(F.col("ds.AvgSickLeaveHours"), 1).alias("DeptAvgSickLeave"),
        F.when(F.col("ed.VacationHours") > F.col("ds.AvgVacationHours"), "Above Average")
        .when(F.col("ed.VacationHours") == F.col("ds.AvgVacationHours"), "Average")
        .otherwise("Below Average").alias("VacationComparison"),
        F.when(F.col("ds.TotalEmployees") > 20, "Large Department")
        .when(F.col("ds.TotalEmployees") > 10, "Medium Department")
        .otherwise("Small Department").alias("DepartmentSize"),
        F.round((F.col("ds.SalariedEmployees") * 100.0) / F.col("ds.TotalEmployees"), 1).alias("SalariedPercentage"),
        F.col("ptb.CountByPayType"),
        F.round(F.col("ptb.AvgVacationByPayType"), 1).alias("PayTypeAvgVacation")
    ) \
    .orderBy("DepartmentName", "LastName", "FirstName")

# ========================================
# MEDIUM QUERY 2: Product Sales Summary by Category
# ========================================
# Basic product analysis with simple aggregations and categorization

# CTE: ProductInfo
product_info = Production_Product.alias("p") \
    .join(Production_ProductSubcategory.alias("ps"), F.col("p.ProductSubcategoryID") == F.col("ps.ProductSubcategoryID"), "left") \
    .join(Production_ProductCategory.alias("pc"), F.col("ps.ProductCategoryID") == F.col("pc.ProductCategoryID"), "left") \
    .where(F.col("p.ListPrice") > 0) \
    .select(
        F.col("p.ProductID"),
        F.col("p.Name").alias("ProductName"),
        F.col("p.ProductNumber"),
        F.col("p.ListPrice"),
        F.col("p.StandardCost"),
        F.col("p.Color"),
        F.col("p.Size"),
        F.col("ps.Name").alias("SubcategoryName"),
        F.col("pc.Name").alias("CategoryName"),
        F.when(F.col("p.ListPrice") > 1000, "High-End")
        .when(F.col("p.ListPrice") > 500, "Mid-Range")
        .when(F.col("p.ListPrice") > 100, "Standard")
        .otherwise("Budget").alias("PriceRange"),
        (F.when(F.col("p.StandardCost") > 0, F.col("p.ListPrice") - F.col("p.StandardCost"))
         .otherwise(0)).alias("ProfitPerUnit")
    )

# CTE: SalesInfo
sales_info = Sales_SalesOrderDetail.alias("sod") \
    .join(Sales_SalesOrderHeader.alias("soh"), F.col("sod.SalesOrderID") == F.col("soh.SalesOrderID")) \
    .where(F.col("soh.OrderDate") >= "2013-01-01") \
    .groupBy("sod.ProductID") \
    .agg(
        F.countDistinct("sod.SalesOrderID").alias("OrderCount"),
        F.sum("sod.OrderQty").alias("TotalQuantitySold"),
        F.sum("sod.LineTotal").alias("TotalRevenue"),
        F.avg("sod.UnitPrice").alias("AvgSellingPrice"),
        F.max("soh.OrderDate").alias("LastSaleDate"),
        F.min("soh.OrderDate").alias("FirstSaleDate"),
        F.countDistinct("soh.CustomerID").alias("UniqueCustomers")
    )

# CTE: CategoryTotals
category_totals = product_info.alias("pi") \
    .join(sales_info.alias("si"), F.col("pi.ProductID") == F.col("si.ProductID"), "left") \
    .where(F.col("pi.CategoryName").isNotNull()) \
    .groupBy("pi.CategoryName") \
    .agg(
        F.count("pi.ProductID").alias("ProductsInCategory"),
        F.sum("si.TotalRevenue").alias("CategoryRevenue"),
        F.avg("pi.ListPrice").alias("AvgCategoryPrice"),
        F.sum("si.TotalQuantitySold").alias("CategoryQuantitySold"),
        F.count("si.ProductID").alias("ProductsWithSales")
    )

# CTE: InventoryInfo
inventory_info = Production_ProductInventory.groupBy("ProductID") \
    .agg(
        F.sum("Quantity").alias("TotalInventory"),
        F.count("LocationID").alias("LocationCount"),
        F.avg("Quantity").alias("AvgInventoryPerLocation")
    )

# Final SELECT statement
final_result2 = product_info.alias("pi") \
    .join(sales_info.alias("si"), F.col("pi.ProductID") == F.col("si.ProductID"), "left") \
    .join(category_totals.alias("ct"), F.col("pi.CategoryName") == F.col("ct.CategoryName"), "left") \
    .join(inventory_info.alias("ii"), F.col("pi.ProductID") == F.col("ii.ProductID"), "left") \
    .where(F.col("pi.CategoryName").isNotNull()) \
    .select(
        F.col("pi.CategoryName"),
        F.col("pi.SubcategoryName"),
        F.col("pi.ProductName"),
        F.col("pi.ProductNumber"),
        F.col("pi.ListPrice"),
        F.col("pi.StandardCost"),
        F.col("pi.ProfitPerUnit"),
        F.col("pi.PriceRange"),
        F.col("pi.Color"),
        F.col("pi.Size"),
        F.coalesce(F.col("si.TotalQuantitySold"), F.lit(0)).alias("QuantitySold"),
        F.coalesce(F.col("si.TotalRevenue"), F.lit(0)).alias("Revenue"),
        F.coalesce(F.col("si.OrderCount"), F.lit(0)).alias("OrderCount"),
        F.coalesce(F.col("si.UniqueCustomers"), F.lit(0)).alias("UniqueCustomers"),
        F.col("si.LastSaleDate"),
        F.coalesce(F.col("ii.TotalInventory"), F.lit(0)).alias("CurrentInventory"),
        F.coalesce(F.col("ii.LocationCount"), F.lit(0)).alias("InventoryLocations"),
        F.col("ct.CategoryRevenue"),
        F.col("ct.ProductsInCategory"),
        F.col("ct.ProductsWithSales"),
        F.round(F.col("ct.AvgCategoryPrice"), 2).alias("AvgCategoryPrice"),
        F.when(F.col("si.TotalQuantitySold").isNull(), "No Sales")
        .when(F.col("si.TotalQuantitySold") > 1000, "High Volume")
        .when(F.col("si.TotalQuantitySold") > 100, "Medium Volume")
        .otherwise("Low Volume").alias("SalesVolume"),
        F.when(F.col("si.LastSaleDate") >= F.date_sub(F.current_date(), 180), "Recent")  # Approximately 6 months
        .when(F.col("si.LastSaleDate") >= F.date_sub(F.current_date(), 365), "Moderate")  # Approximately 1 year
        .when(F.col("si.LastSaleDate").isNotNull(), "Old")
        .otherwise("Never Sold").alias("SalesRecency"),
        F.when(F.col("ii.TotalInventory") > 1000, "High Stock")
        .when(F.col("ii.TotalInventory") > 100, "Medium Stock")
        .when(F.col("ii.TotalInventory") > 0, "Low Stock")
        .otherwise("No Stock").alias("InventoryLevel"),
        F.round((F.coalesce(F.col("si.TotalRevenue"), F.lit(0)) * 100.0) / F.nullif(F.col("ct.CategoryRevenue"), F.lit(0)), 2).alias("PercentOfCategoryRevenue")
    ) \
    .orderBy("CategoryName", F.desc("Revenue"), "ProductName")