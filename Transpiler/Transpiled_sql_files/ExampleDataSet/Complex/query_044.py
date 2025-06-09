from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, rank, desc, lit
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SQLtoPySpark").getOrCreate()

orders_df = spark.table("orders")
stores_df = spark.table("stores")

sales_cte = orders_df.join(stores_df, "store_id")
.filter((col("order_date") >= lit("2024-01-01")) & (col("order_date") <= lit("2024-12-31")))
.groupBy("store_id")
.agg(sum("total_amount").alias("total_sales"))

avg_total_sales_val = sales_cte.agg(avg("total_sales")).collect()[0][0]

top_stores = sales_cte.filter(col("total_sales") > avg_total_sales_val).select("store_id")

joined_df = stores_df.join(top_stores, "store_id")
.join(sales_cte.select("store_id", "total_sales"), "store_id")

rank_window_spec = Window.partitionBy("region").orderBy(desc("total_sales"))

result_df = joined_df.filter(col("region").isNotNull())
.withColumn("sales_rank", rank().over(rank_window_spec))
.select(
col("store_id"),
col("region"),
col("total_sales"),
col("sales_rank")
)

result_df.show()
spark.stop()