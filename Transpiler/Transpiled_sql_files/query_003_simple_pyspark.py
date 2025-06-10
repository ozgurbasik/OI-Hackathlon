"""
PySpark code transpiled from query_003.sql
Classification: Simple
Generated on: 2025-06-10 19:27:46
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

Sales_SalesOrderHeader = spark.read.parquet("/mnt/advdata/AdventureWorks/Sales_SalesOrderHeader.parquet")

# Filter the DataFrame based on the specified conditions
filtered_sales_order_header = Sales_SalesOrderHeader.filter(
    (Sales_SalesOrderHeader.OrderDate >= '2014-01-01') &
    (Sales_SalesOrderHeader.OnlineOrderFlag == 1) &
    (Sales_SalesOrderHeader.TotalDue > 500)
)

# Calculate the difference in days between OrderDate and ShipDate
# Handle potential null ShipDate by defaulting to OrderDate if ShipDate is null
days_to_ship = F.when(F.col("ShipDate").isNull(), 0).otherwise(F.datediff(F.col("ShipDate"), F.col("OrderDate")))

# Define the conditions for OrderSize using a CASE WHEN statement equivalent
order_size = F.when(F.col("TotalDue") > 5000, 'Large Order') \
    .when(F.col("TotalDue") > 1000, 'Medium Order') \
    .otherwise('Small Order')

# Select the required columns and apply the calculated fields
final_sales_order_header = filtered_sales_order_header.select(
    "SalesOrderID",
    "CustomerID",
    "OrderDate",
    "DueDate",
    "ShipDate",
    "SubTotal",
    "TaxAmt",
    "Freight",
    "TotalDue",
    days_to_ship.alias("DaysToShip"),
    order_size.alias("OrderSize")
).orderBy(F.col("OrderDate").desc())

# The final_sales_order_header DataFrame now contains the transformed data
# In order to view the results, you'd uncomment the following line
# final_sales_order_header.show()