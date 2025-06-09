from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQLtoPySpark").getOrCreate()

customers_df = spark.table("customers")

result_df = customers_df.filter(customers_df["active"] == 1).select("id", "name")

result_df.show()
spark.stop()