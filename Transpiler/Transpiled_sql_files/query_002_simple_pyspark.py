"""
PySpark code transpiled from query_002.sql
Classification: Simple
Generated on: 2025-06-10 17:37:10
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'

def transform_product_data(product_df):
    """
    Transforms the product data based on the provided SQL query logic.

    Args:
        product_df: PySpark DataFrame representing the Production.Product table.

    Returns:
        PySpark DataFrame containing the transformed product data.
    """

    # Filter the DataFrame based on ListPrice, SellEndDate, and Color
    filtered_product_df = product_df.filter(
        (F.col("ListPrice") > 100) & (F.col("SellEndDate").isNull()) & (F.col("Color").isNotNull())
    )

    # Define the price category based on ListPrice using a CASE WHEN equivalent
    categorized_product_df = filtered_product_df.withColumn(
        "PriceCategory",
        F.when(F.col("ListPrice") > 1000, "Premium")
        .when(F.col("ListPrice") > 500, "Mid-Range")
        .otherwise("Standard")
    )

    # Select and rename the required columns
    transformed_product_df = categorized_product_df.select(
        F.col("ProductID"),
        F.col("Name").alias("ProductName"),
        F.col("ProductNumber"),
        F.col("ListPrice"),
        F.col("StandardCost"),
        F.col("Color"),
        F.col("Size"),
        F.col("Weight"),
        F.col("DaysToManufacture"),
        F.col("PriceCategory")
    )

    # Order the result by ListPrice in descending order
    ordered_product_df = transformed_product_df.orderBy(F.col("ListPrice").desc())

    return ordered_product_df


if __name__ == '__main__':
    spark = SparkSession.builder.appName("ProductDataTransformation").getOrCreate()

    # Create a sample DataFrame (replace with your actual data source)
    data = [
        (1, "Product A", "PA123", 1200.00, 600.00, "Red", "M", 0.5, 5, None),
        (2, "Product B", "PB456", 600.00, 300.00, "Blue", "S", 0.3, 3, None),
        (3, "Product C", "PC789", 200.00, 100.00, "Green", "L", 0.7, 7, None),
        (4, "Product D", "PD012", 1500.00, 750.00, "Black", "XL", 0.9, 9, "2023-12-31"),
        (5, "Product E", "PE345", 80.00, 40.00, None, "M", 0.4, 4, None),
    ]

    schema = T.StructType([
        T.StructField("ProductID", T.IntegerType(), True),
        T.StructField("Name", T.StringType(), True),
        T.StructField("ProductNumber", T.StringType(), True),
        T.StructField("ListPrice", T.DoubleType(), True),
        T.StructField("StandardCost", T.DoubleType(), True),
        T.StructField("Color", T.StringType(), True),
        T.StructField("Size", T.StringType(), True),
        T.StructField("Weight", T.DoubleType(), True),
        T.StructField("DaysToManufacture", T.IntegerType(), True),
        T.StructField("SellEndDate", T.StringType(), True),
    ])

    product_df = spark.createDataFrame(data, schema=schema)

    # Transform the product data
    transformed_product_df = transform_product_data(product_df)

    # Show the transformed data
    transformed_product_df.show()

    spark.stop()