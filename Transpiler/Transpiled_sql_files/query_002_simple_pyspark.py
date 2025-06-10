"""
PySpark code transpiled from query_002.sql
Classification: Simple
Generated on: 2025-06-10 19:27:42
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'

# Define the path to the Production_Product parquet file
Production_Product_path = "/mnt/advdata/AdventureWorks/Production_Product.parquet"

# Read the Production.Product table into a DataFrame
Production_Product_df = spark.read.parquet(Production_Product_path)

# Apply transformations and filter conditions
transformed_product_df = Production_Product_df.filter(
    (F.col("ListPrice") > 100) & (F.col("SellEndDate").isNull()) & (F.col("Color").isNotNull())
).select(
    F.col("ProductID"),
    F.col("Name").alias("ProductName"),
    F.col("ProductNumber"),
    F.col("ListPrice"),
    F.col("StandardCost"),
    F.col("Color"),
    F.col("Size"),
    F.col("Weight"),
    F.col("DaysToManufacture"),
    F.when(F.col("ListPrice") > 1000, "Premium")
    .when(F.col("ListPrice") > 500, "Mid-Range")
    .otherwise("Standard")
    .alias("PriceCategory"),
).orderBy(F.col("ListPrice").desc())

# The transformed_product_df DataFrame now holds the result equivalent to the SQL query
# You can perform further actions with this DataFrame, such as displaying the data

# Return the transformed DataFrame
transformed_product_df