from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc, avg, lag, row_number, when, date_trunc, add_months, current_date, expr
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SQLtoPySpark").getOrCreate()

sales_df = spark.table("sales")
salespeople_df = spark.table("salespeople")

monthly_sales_df = sales_df.filter(col("sale_date") >= add_months(current_date(), -12))
.groupBy("salesperson_id", date_trunc("month", col("sale_date")).alias("sale_month"))
.agg(
sum("amount").alias("monthly_total"),
count("id").alias("transaction_count")
)

rank_window_spec = Window.partitionBy("sale_month").orderBy(desc("monthly_total"))
lag_window_spec = Window.partitionBy("salesperson_id").orderBy("sale_month")

salesperson_rankings_df = monthly_sales_df.withColumn("monthly_rank", row_number().over(rank_window_spec))
.withColumn("prev_month_sales", lag("monthly_total", 1).over(lag_window_spec))

performance_metrics_df = salesperson_rankings_df.withColumn("growth_percentage",
when(col("prev_month_sales").isNull(), 0)
.otherwise(
when(col("prev_month_sales") == 0, None) # Handle division by zero explicitly if needed, NULL is often the result
.otherwise((col("monthly_total") - col("prev_month_sales")) / col("prev_month_sales") * 100)
)
)

agg_window_spec = Window.partitionBy("salesperson_id")

result_df = salespeople_df.join(performance_metrics_df, salespeople_df["id"] == performance_metrics_df["salesperson_id"])
.filter(salespeople_df["active"] == 1)
.withColumn("avg_monthly_sales", avg(col("monthly_total")).over(agg_window_spec))
.withColumn("total_yearly_sales", sum(col("monthly_total")).over(agg_window_spec))
.select(
salespeople_df["id"],
salespeople_df["first_name"],
salespeople_df["last_name"],
performance_metrics_df["sale_month"],
performance_metrics_df["monthly_total"],
performance_metrics_df["transaction_count"],
performance_metrics_df["monthly_rank"],
performance_metrics_df["growth_percentage"],
col("avg_monthly_sales"),
col("total_yearly_sales")
)
.orderBy(desc("sale_month"), col("monthly_rank").asc())

result_df.show()
spark.stop()