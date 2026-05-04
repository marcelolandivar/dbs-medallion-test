
# =========================
# SPARK DECLARATIVE PIPELINES (Modern Version)
# =========================
import sys

from pyspark import pipelines as dp
from config.config import get_config
from config.schema_config import get_traffic_schema, get_roads_schema
from common.transformations import (
    remove_Dups, handle_NULLs, ev_Count, Motor_Count, 
    create_TransformedTime, create_VehicleIntensity, create_LoadTime, 
    road_Category, road_Type
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, sum, current_timestamp

env = spark.conf.get("env", "dev")
print(f"Running with environment: {env}")
cfg = get_config(env, "bronze")

# =========================
# BRONZE LAYER - Auto Loader Ingestion (Streaming Tables)
# Data Quality: Warn on issues but keep all raw data
# =========================

@dp.table(name=f"{cfg.catalog}.bronze.raw_traffic_dlt")
@dp.expect_all({
    "valid_extract_time": "Extract_Time IS NOT NULL",
    "has_count_point_id": "Count_point_id IS NOT NULL",
    "has_record_id": "Record_ID IS NOT NULL"
})
def bronze_traffic():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option('cloudFiles.schemaLocation', f'{cfg.checkpoint}/rawTrafficLoadDLT/schemaInfer')
        .option('header', 'true')
        .schema(get_traffic_schema())
        .load(cfg.landing + '/raw_traffic/')
        .withColumn("Extract_Time", current_timestamp())
    )

@dp.table(name=f"{cfg.catalog}.bronze.raw_roads_dlt")
@dp.expect_all({
    "valid_extract_time": "Extract_Time IS NOT NULL",
    "has_road_id": "Road_ID IS NOT NULL",
    "has_road_category": "Road_Category IS NOT NULL"
})
def bronze_roads():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option('cloudFiles.schemaLocation', f'{cfg.checkpoint}/rawRoadsLoadDLT/schemaInfer')
        .option('header', 'true')
        .schema(get_roads_schema())
        .load(cfg.landing + '/raw_roads/')
        .withColumn("Extract_Time", current_timestamp())
    )

# =========================
# SILVER LAYER - Data Cleaning (Streaming Tables)
# Data Quality: Drop invalid records after cleaning
# =========================

@dp.table(name=f"{cfg.catalog}.silver.roads_dlt")
@dp.expect_all_or_drop({
    "valid_road_id": "Road_ID IS NOT NULL",
    "valid_road_category": "Road_Category IS NOT NULL"
})
@dp.expect("road_category_standard", "Road_Category IN ('TA', 'TM', 'PA', 'PM', 'M')")
def silver_roads():
    df = spark.readStream.table(f"{cfg.catalog}.{cfg.schema}.raw_roads_dlt")
    df = remove_Dups(df)
    all_columns = df.schema.names
    df_clean = handle_NULLs(df, all_columns)
    return df_clean

@dp.table(name=f"{cfg.catalog}.silver.traffic_dlt")
@dp.expect_all_or_drop({
    "valid_count_point": "Count_point_id IS NOT NULL",
    "valid_transformed_time": "Transformed_Time IS NOT NULL",
    "non_negative_ev_count": "Electric_Vehicles_Count >= 0",
    "non_negative_motor_count": "Motor_Vehicles_Count >= 0"
})
@dp.expect("reasonable_vehicle_counts", "Motor_Vehicles_Count < 10000")
def silver_traffic():
    df = spark.readStream.table(f"{cfg.catalog}.{cfg.schema}.raw_traffic_dlt")
    df_traffic_data = remove_Dups(df)
    all_columns = df_traffic_data.schema.names
    df_traffic_data = handle_NULLs(df_traffic_data, all_columns)
    df_traffic_data = ev_Count(df_traffic_data)
    df_traffic_data = Motor_Count(df_traffic_data)
    df_clean = create_TransformedTime(df_traffic_data)
    return df_clean

# =========================
# GOLD LAYER - Business Logic (Streaming Tables)
# Data Quality: Fail on critical business rule violations
# =========================

@dp.table(name=f"{cfg.catalog}.gold.roads_dlt")
@dp.expect_or_fail("load_time_exists", "Load_Time IS NOT NULL")
@dp.expect_all({
    "recent_load": "Load_Time >= current_date() - INTERVAL 7 DAYS",
    "valid_road_data": "Road_ID IS NOT NULL AND Road_Category IS NOT NULL"
})
def gold_roads():
    df = spark.readStream.table(f"{cfg.catalog}.silver.roads_dlt")
    df_clean = create_LoadTime(df)
    return df_clean

@dp.table(name=f"{cfg.catalog}.gold.traffic_dlt")
@dp.expect_or_fail("load_time_exists", "Load_Time IS NOT NULL")
@dp.expect_all_or_drop({
    "valid_vehicle_intensity": "Vehicle_Intensity IS NOT NULL AND Vehicle_Intensity >= 0",
    "complete_record": "Count_point_id IS NOT NULL AND Transformed_Time IS NOT NULL"
})
@dp.expect("reasonable_intensity", "Vehicle_Intensity < 1000")
def gold_traffic():
    df = spark.readStream.table(f"{cfg.catalog}.silver.traffic_dlt")
    df_traffic = create_VehicleIntensity(df)
    df_clean = create_LoadTime(df_traffic)
    return df_clean

