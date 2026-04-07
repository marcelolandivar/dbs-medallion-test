# =========================
# DELTA LIVE TABLES (DLT) VERSION
# =========================
import dlt
from config.config import get_config
from config.schema_config import get_traffic_schema, get_roads_schema
from common.transformations import remove_Dups, handle_NULLs, ev_Count, Motor_Count, create_TransformedTime, create_VehicleIntensity, create_LoadTime, road_Category, road_Type
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, sum, current_timestamp
import os

env = os.environ.get("ENV", "dev")  # Default to 'dev' if ENV variable is not set 
cfg = get_config(env, "bronze")

@dlt.table(name=f"bronze.raw_traffic_dlt")
def bronze_traffic():
    return  (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{cfg.checkpoint}/rawTrafficLoadDLT/schemaInfer')
        .option('header','true')
        .schema(get_traffic_schema())
        .load(cfg.landing+'/raw_traffic/')
        .withColumn("Extract_Time", current_timestamp())
    )

@dlt.table(name=f"bronze.raw_roads_dlt")
def bronze_roads():
    return  (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{cfg.checkpoint}/rawRoadsLoadDLT/schemaInfer')
        .option('header','true')
        .schema(get_roads_schema())
        .load(cfg.landing+'/raw_roads/')
        .withColumn("Extract_Time", current_timestamp())
    )

@dlt.table(name="silver.silver_roads")
def silver_orders():
    df = dlt.read("bronze.raw_roads_dlt")
    df = remove_Dups(df)
    AllColumns = df.schema.names
    df_clean = handle_NULLs(df,AllColumns)
    return (
        df_clean
        .option('checkpointLocation', cfg.checkpoint+ "/SilverRoadsLoadDLT/Checkpt/")
        .outputMode('append')
        .queryName("SilverRoadsWriteStreamDLT")
    )

@dlt.table(name="silver.silver_traffic")
def silver_orders():
    df = dlt.read("bronze.raw_traffic_dlt")
    df_traffic_data = remove_Dups(df)
    Allcolumns =df_traffic_data.schema.names
    df_traffic_data = handle_NULLs(df_traffic_data, Allcolumns)
    df_traffic_data = ev_Count(df_traffic_data)
    df_traffic_data = Motor_Count(df_traffic_data)
    df_clean = create_TransformedTime(df_traffic_data)
    return (
        df_clean
        .option('checkpointLocation', cfg.checkpoint+ "/SilverTrafficLoadDLT/Checkpt/")
        .outputMode('append')
        .queryName("SilverTrafficWriteStreamDLT")
    )

@dlt.table(name="gold.gold_roads_dlt")
def gold_roads():
    df = dlt.read("silver.silver_traffic_dlt")
    df_clean = create_LoadTime(df)
    return (
        df_clean
        .option('checkpointLocation', cfg.checkpoint+ "/GoldRoadsLoadDLT/Checkpt/")
                .outputMode('append')
                .queryName("GoldRoadsWriteStreamDLT")
    )

@dlt.table(name="gold.gold_traffic_dlt")
def gold_traffic():
    df = dlt.read("silver.silver_traffic_dlt")
    df_traffic = create_VehicleIntensity(df)
    df_clean = create_LoadTime(df_traffic)
    return (
        df_clean
        .option('checkpointLocation', cfg.checkpoint+ "/GoldTrafficLoadDLT/Checkpt/")
                .outputMode('append')
                .queryName("GoldTrafficWriteStreamDLT")
    )