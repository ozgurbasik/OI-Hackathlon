"""
PySpark code transpiled from query_003.sql
Classification: Simple
Generated on: 2025-06-10 17:37:14
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'
# spark = SparkSession.builder.appName("Query003").getOrCreate()

def solve():
    """
    This function converts the provided SQL query to PySpark DataFrame API code.
    """

    # Read the SalesOrderHeader table into a DataFrame
    sales_order_header = spark.table("Sales.SalesOrderHeader")

    # Filter the data based on the WHERE clause conditions
    filtered_sales_order_header = sales_order_header.filter(
        (F.col("OrderDate") >= "2014-01-01") &
        (F.col("OnlineOrderFlag") == 1) &
        (F.col("TotalDue") > 500)
    )

    # Calculate the DaysToShip column using datediff function
    days_to_ship = F.datediff(F.col("OrderDate"), F.col("ShipDate")).alias("DaysToShip")

    # Define the conditions for the OrderSize column using a CASE WHEN statement
    order_size_condition = F.when(F.col("TotalDue") > 5000, "Large Order") \
        .when(F.col("TotalDue") > 1000, "Medium Order") \
        .otherwise("Small Order") \
        .alias("OrderSize")

    # Select the required columns and apply the transformations
    final_result = filtered_sales_order_header.select(
        "SalesOrderID",
        "CustomerID",
        "OrderDate",
        "DueDate",
        "ShipDate",
        "SubTotal",
        "TaxAmt",
        "Freight",
        "TotalDue",
        days_to_ship,
        order_size_condition
    ).orderBy(F.col("OrderDate").desc())

    return final_result

# Example usage:
# result_df = solve()
# result_df.show()