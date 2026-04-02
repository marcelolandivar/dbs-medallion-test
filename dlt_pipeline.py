# =========================
# DELTA LIVE TABLES (DLT) VERSION
# =========================
import dlt
from config.config import get_config
from config.schema_config import get_traffic_schema
from pyspark.sql.functions import col, to_timestamp, sum, current_timestamp
import os

env = os.environ.get("ENV", "dev")  # Default to 'dev' if ENV variable is not set 
cfg = get_config(env, "bronze")

@dlt.table(name=f"{cfg.schema}.raw_traffic_dlt")
def bronze_traffic():
    return  (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{cfg.checkpoint}/rawTrafficLoad/schemaInfer')
        .option('header','true')
        .schema(get_traffic_schema())
        .load(cfg.landing+'/raw_traffic/')
        .withColumn("Extract_Time", current_timestamp())
    )

@dlt.table(name=f"{cfg.schema}.raw_traffic_dlt")
def bronze_traffic():
    return  (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{cfg.checkpoint}/rawTrafficLoad/schemaInfer')
        .option('header','true')
        .schema(get_traffic_schema())
        .load(cfg.landing+'/raw_traffic/')
        .withColumn("Extract_Time", current_timestamp())
    )

@dlt.table(name="silver")
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

