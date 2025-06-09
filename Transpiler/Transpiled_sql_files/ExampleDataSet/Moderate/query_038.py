from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_sub

spark = SparkSession.builder.appName("SQLtoPySpark").getOrCreate()

customers_df = spark.table("customers")
orders_df = spark.table("orders")

recent_orders_df = orders_df.filter(orders_df["order_date"] > date_sub(current_date(), 30))

joined_df = customers_df.join(recent_orders_df, customers_df["id"] == recent_orders_df["customer_id"])

result_df = joined_df.filter(joined_df["active"] == 1).select(customers_df["id"], customers_df["name"], recent_orders_df["order_date"])

result_df.show()
spark.stop()