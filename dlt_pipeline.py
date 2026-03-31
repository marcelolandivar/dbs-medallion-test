# =========================
# DELTA LIVE TABLES (DLT) VERSION
# =========================
import dlt
from pyspark.sql.functions import col, to_timestamp, sum

# NOTE: In DLT, config is usually passed via pipeline settings (not Python files)

@dlt.table(name="bronze_orders")
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/mnt/${env}/raw/orders")
    )


@dlt.table(name="silver_orders")
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
def silver_orders():
    df = dlt.read("bronze_orders")

    return (
        df
        .withColumn("order_ts", to_timestamp(col("order_ts")))
        .withColumnRenamed("id", "order_id")
        .dropDuplicates(["order_id"])
    )


@dlt.table(name="gold_sales")
def gold_sales():
    return (
        dlt.read("silver_orders")
        .groupBy("customer_id")
        .agg(sum("amount").alias("total_spent"))
    )

