"""
PySpark code transpiled from query_001.sql
Classification: Simple
Generated on: 2025-06-10 17:37:03
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assume SparkSession is already created as 'spark'

def transform_data(spark):

    # Read the Production.Product table into a DataFrame
    product_df = spark.read.table("Production.Product")

    # Select and rename columns
    transformed_df = product_df.select(
        F.col("ProductID"),
        F.col("Name").alias("ProductName"),
        F.col("ProductNumber"),
        F.col("ListPrice"),
        F.col("Color")
    )

    return transformed_df


if __name__ == "__main__":
    # Create a SparkSession (replace with your actual SparkSession if needed)
    spark = SparkSession.builder.appName("Example").getOrCreate()

    # Example Usage: Replace with your actual table name
    try:
        # Create dummy table
        data = [
                (1, "Product A", "PA123", 100.0, "Red"),
                (2, "Product B", "PB456", 200.0, "Blue"),
                (3, "Product C", "PC789", 150.0, "Green")
            ]
        schema = ["ProductID", "Name", "ProductNumber", "ListPrice", "Color"]

        sample_df = spark.createDataFrame(data, schema=schema)
        sample_df.createOrReplaceTempView("Production.Product")

        result_df = transform_data(spark)
        result_df.show()
    finally:
        spark.stop()