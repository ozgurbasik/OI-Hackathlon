from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, rank, desc, lit, date_trunc
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SQLtoPySpark").getOrCreate()

orders_df = spark.table("orders")
stores_df = spark.table("stores")

sales_cte = orders_df.join(stores_df, orders_df["store_id"] == stores_df["store_id"])
.filter((col("order_date") >= lit("2024-01-01")) & (col("order_date") <= lit("2024-12-31")))
.groupBy(stores_df["store_id"])
.agg(sum(orders_df["total_amount"]).alias("total_sales"))

avg_total_sales = sales_cte.agg(avg("total_sales")).collect()[0][0]

top_stores_sales = sales_cte.filter(col("total_sales") > avg_total_sales)

rank_window_spec = Window.partitionBy("region").orderBy(desc("total_sales"))

result_df = stores_df.join(top_stores_sales, stores_df["store_id"] == top_stores_sales["store_id"])
.filter(col("region").isNotNull())
.withColumn("sales_rank", rank().over(rank_window_spec))
.select(
stores_df["store_id"],
stores_df["region"],
top_stores_sales["total_sales"],
col("sales_rank")
)

result_df.show()
spark.stop()