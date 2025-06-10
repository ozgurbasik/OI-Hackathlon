"""
PySpark code transpiled from query_006.sql
Classification: Complex
Generated on: 2025-06-10 17:38:12
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'

# Define the EmployeeDetails CTE
employee_details = spark.sql("""
    SELECT 
        e.BusinessEntityID,
        p.FirstName,
        p.LastName,
        e.JobTitle,
        e.HireDate,
        e.VacationHours,
        e.SickLeaveHours,
        e.SalariedFlag,
        dh.DepartmentID,
        d.Name AS DepartmentName
    FROM HumanResources.Employee e
    INNER JOIN Person.Person p ON e.BusinessEntityID = p.BusinessEntityID
    INNER JOIN HumanResources.EmployeeDepartmentHistory dh ON e.BusinessEntityID = dh.BusinessEntityID
    INNER JOIN HumanResources.Department d ON dh.DepartmentID = d.DepartmentID
    WHERE e.CurrentFlag = 1 
        AND dh.EndDate IS NULL
""")

employee_details = employee_details.withColumn(
    'PayType',
    F.when(F.col('SalariedFlag') == 1, 'Salaried').otherwise('Hourly')
).withColumn(
    'ExperienceLevel',
    F.when(F.year(F.current_date()) - F.year(F.col('HireDate')) >= 10, 'Senior')
    .when(F.year(F.current_date()) - F.year(F.col('HireDate')) >= 5, 'Mid-Level')
    .otherwise('Junior')
)

# Define the DepartmentStats CTE
department_stats = employee_details.groupBy('DepartmentName').agg(
    F.count('*').alias('TotalEmployees'),
    F.sum(F.when(F.col('PayType') == 'Salaried', 1).otherwise(0)).alias('SalariedEmployees'),
    F.sum(F.when(F.col('PayType') == 'Hourly', 1).otherwise(0)).alias('HourlyEmployees'),
    F.sum(F.when(F.col('ExperienceLevel') == 'Senior', 1).otherwise(0)).alias('SeniorEmployees'),
    F.sum(F.when(F.col('ExperienceLevel') == 'Mid-Level', 1).otherwise(0)).alias('MidLevelEmployees'),
    F.sum(F.when(F.col('ExperienceLevel') == 'Junior', 1).otherwise(0)).alias('JuniorEmployees'),
    F.avg('VacationHours').alias('AvgVacationHours'),
    F.avg('SickLeaveHours').alias('AvgSickLeaveHours'),
    F.max('VacationHours').alias('MaxVacationHours'),
    F.min('VacationHours').alias('MinVacationHours')
)

# Define the PayTypeBreakdown CTE
pay_type_breakdown = employee_details.groupBy('DepartmentName', 'PayType').agg(
    F.count('*').alias('CountByPayType'),
    F.avg('VacationHours').alias('AvgVacationByPayType'),
    F.avg('SickLeaveHours').alias('AvgSickLeaveByPayType')
)

# Final SELECT statement
final_df = employee_details.alias('ed').join(department_stats.alias('ds'), F.col('ed.DepartmentName') == F.col('ds.DepartmentName')) \
    .join(pay_type_breakdown.alias('ptb'), (F.col('ed.DepartmentName') == F.col('ptb.DepartmentName')) & (F.col('ed.PayType') == F.col('ptb.PayType'))) \
    .select(
        F.col('ed.DepartmentName'),
        F.col('ed.FirstName'),
        F.col('ed.LastName'),
        F.col('ed.JobTitle'),
        F.col('ed.HireDate'),
        F.col('ed.PayType'),
        F.col('ed.ExperienceLevel'),
        F.col('ed.VacationHours'),
        F.col('ed.SickLeaveHours'),
        F.col('ds.TotalEmployees'),
        F.col('ds.SalariedEmployees'),
        F.col('ds.HourlyEmployees'),
        F.col('ds.SeniorEmployees'),
        F.col('ds.MidLevelEmployees'),
        F.col('ds.JuniorEmployees'),
        F.round(F.col('ds.AvgVacationHours'), 1).alias('DeptAvgVacation'),
        F.round(F.col('ds.AvgSickLeaveHours'), 1).alias('DeptAvgSickLeave'),
        F.when(F.col('ed.VacationHours') > F.col('ds.AvgVacationHours'), 'Above Average') \
            .when(F.col('ed.VacationHours') == F.col('ds.AvgVacationHours'), 'Average') \
            .otherwise('Below Average').alias('VacationComparison'),
        F.when(F.col('ds.TotalEmployees') > 20, 'Large Department') \
            .when(F.col('ds.TotalEmployees') > 10, 'Medium Department') \
            .otherwise('Small Department').alias('DepartmentSize'),
        F.round((F.col('ds.SalariedEmployees') * 100.0) / F.col('ds.TotalEmployees'), 1).alias('SalariedPercentage'),
        F.col('ptb.CountByPayType'),
        F.round(F.col('ptb.AvgVacationByPayType'), 1).alias('PayTypeAvgVacation')
    ).orderBy('DepartmentName', 'LastName', 'FirstName')



#========================================
# MEDIUM QUERY 2: Product Sales Summary by Category
#========================================

# ProductInfo CTE
product_info = spark.sql("""
    SELECT 
        p.ProductID,
        p.Name AS ProductName,
        p.ProductNumber,
        p.ListPrice,
        p.StandardCost,
        p.Color,
        p.Size,
        ps.Name AS SubcategoryName,
        pc.Name AS CategoryName
    FROM Production.Product p
    LEFT JOIN Production.ProductSubcategory ps ON p.ProductSubcategoryID = ps.ProductSubcategoryID
    LEFT JOIN Production.ProductCategory pc ON ps.ProductCategoryID = pc.ProductCategoryID
    WHERE p.ListPrice > 0
""")

product_info = product_info.withColumn(
    'PriceRange',
    F.when(F.col('ListPrice') > 1000, 'High-End')
    .when(F.col('ListPrice') > 500, 'Mid-Range')
    .when(F.col('ListPrice') > 100, 'Standard')
    .otherwise('Budget')
).withColumn(
    'ProfitPerUnit',
    F.when(F.col('StandardCost') > 0, F.col('ListPrice') - F.col('StandardCost')).otherwise(0)
)

# SalesInfo CTE
sales_info = spark.sql("""
    SELECT 
        sod.ProductID,
        sod.SalesOrderID,
        sod.OrderQty,
        sod.LineTotal,
        sod.UnitPrice,
        soh.OrderDate,
        soh.CustomerID
    FROM Sales.SalesOrderDetail sod
    INNER JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
    WHERE soh.OrderDate >= '2013-01-01'
""")

sales_info_agg = sales_info.groupBy('ProductID').agg(
    F.countDistinct('SalesOrderID').alias('OrderCount'),
    F.sum('OrderQty').alias('TotalQuantitySold'),
    F.sum('LineTotal').alias('TotalRevenue'),
    F.avg('UnitPrice').alias('AvgSellingPrice'),
    F.max('OrderDate').alias('LastSaleDate'),
    F.min('OrderDate').alias('FirstSaleDate'),
    F.countDistinct('CustomerID').alias('UniqueCustomers')
)

# CategoryTotals CTE
category_totals = product_info.join(sales_info_agg, 'ProductID', 'left') \
    .filter(F.col('CategoryName').isNotNull()) \
    .groupBy('CategoryName').agg(
        F.countDistinct('ProductID').alias('ProductsInCategory'),
        F.sum('TotalRevenue').alias('CategoryRevenue'),
        F.avg('ListPrice').alias('AvgCategoryPrice'),
        F.sum('TotalQuantitySold').alias('CategoryQuantitySold'),
        F.count('ProductID').alias('ProductsWithSales')
    )

# InventoryInfo CTE
inventory_info = spark.sql("""
    SELECT 
        ProductID,
        Quantity,
        LocationID
    FROM Production.ProductInventory
""")

inventory_info_agg = inventory_info.groupBy('ProductID').agg(
    F.sum('Quantity').alias('TotalInventory'),
    F.countDistinct('LocationID').alias('LocationCount'),
    F.avg('Quantity').alias('AvgInventoryPerLocation')
)

# Final SELECT statement with LEFT JOINs and CASE statements
final_product_df = product_info.alias('pi') \
    .join(sales_info_agg.alias('si'), 'ProductID', 'left') \
    .join(category_totals.alias('ct'), 'CategoryName', 'left') \
    .join(inventory_info_agg.alias('ii'), 'ProductID', 'left') \
    .select(
        F.col('pi.CategoryName'),
        F.col('pi.SubcategoryName'),
        F.col('pi.ProductName'),
        F.col('pi.ProductNumber'),
        F.col('pi.ListPrice'),
        F.col('pi.StandardCost'),
        F.col('pi.ProfitPerUnit'),
        F.col('pi.PriceRange'),
        F.col('pi.Color'),
        F.col('pi.Size'),
        F.coalesce(F.col('si.TotalQuantitySold'), F.lit(0)).alias('QuantitySold'),
        F.coalesce(F.col('si.TotalRevenue'), F.lit(0)).alias('Revenue'),
        F.coalesce(F.col('si.OrderCount'), F.lit(0)).alias('OrderCount'),
        F.coalesce(F.col('si.UniqueCustomers'), F.lit(0)).alias('UniqueCustomers'),
        F.col('si.LastSaleDate'),
        F.coalesce(F.col('ii.TotalInventory'), F.lit(0)).alias('CurrentInventory'),
        F.coalesce(F.col('ii.LocationCount'), F.lit(0)).alias('InventoryLocations'),
        F.col('ct.CategoryRevenue'),
        F.col('ct.ProductsInCategory'),
        F.col('ct.ProductsWithSales'),
        F.round(F.col('ct.AvgCategoryPrice'), 2).alias('AvgCategoryPrice'),
        F.when(F.col('si.TotalQuantitySold').isNull(), 'No Sales') \
            .when(F.col('si.TotalQuantitySold') > 1000, 'High Volume') \
            .when(F.col('si.TotalQuantitySold') > 100, 'Medium Volume') \
            .otherwise('Low Volume').alias('SalesVolume'),
        F.when(F.col('si.LastSaleDate') >= F.add_months(F.current_date(), -6), 'Recent') \
            .when(F.col('si.LastSaleDate') >= F.add_months(F.current_date(), -12), 'Moderate') \
            .when(F.col('si.LastSaleDate').isNotNull(), 'Old') \
            .otherwise('Never Sold').alias('SalesRecency'),
        F.when(F.col('ii.TotalInventory') > 1000, 'High Stock') \
            .when(F.col('ii.TotalInventory') > 100, 'Medium Stock') \
            .when(F.col('ii.TotalInventory') > 0, 'Low Stock') \
            .otherwise('No Stock').alias('InventoryLevel'),
        (F.coalesce(F.col('si.TotalRevenue'), F.lit(0)) * 100.0 / F.nullif(F.col('ct.CategoryRevenue'), 0)).alias('PercentOfCategoryRevenue')
    ).filter(F.col('CategoryName').isNotNull()) \
    .orderBy(F.col('CategoryName'), F.col('Revenue').desc(), F.col('ProductName'))