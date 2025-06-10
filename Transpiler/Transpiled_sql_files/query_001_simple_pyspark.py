"""
PySpark code transpiled from query_001.sql
Classification: Simple
Generated on: 2025-06-10 19:27:39
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Define paths
Production_Product_path = "/mnt/advdata/AdventureWorks/Production_Product.parquet"

# Read data into DataFrames
production_product_df = spark.read.parquet(Production_Product_path)

# Select required columns and rename
selected_products_df = production_product_df.select(
    F.col("ProductID"),
    F.col("Name").alias("ProductName"),
    F.col("ProductNumber"),
    F.col("ListPrice"),
    F.col("Color")
)