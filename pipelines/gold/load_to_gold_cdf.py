# =========================
# pipelines/gold/gold_orders.py (with metadata tracking)
# =========================
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, sum as _sum, avg as _avg, count as _count
from config.config import get_config
from common.transformations import create_LoadTime, create_VehicleIntensity
from common.validations import safe_transform

def read_SilverTraffic_CDF(spk, cfg, start_version=0):
    """Read CDF from silver traffic table"""
    print(f'Reading CDF from silver.silver_traffic_cdf (starting version: {start_version})')
    
    df_silverTraffic = (spk.readStream
                        .format("delta")
                        .option("readChangeFeed", "true")
                        .option("startingVersion", start_version)
                        .table(f"`{cfg.catalog}`.`silver`.`silver_traffic_cdf`"))
    
    # Filter for inserts and updates
    df_silverTraffic = (df_silverTraffic
                        .filter(col("_change_type").isin(['insert', 'update_postimage']))
                        .drop('_change_type', '_commit_version', '_commit_timestamp'))
    
    print('✓ Reading silver_traffic Success!')
    return df_silverTraffic


def read_SilverRoads_CDF(spk, cfg, start_version=0):
    """Read CDF from silver roads table"""
    print(f'Reading CDF from silver.silver_roads_cdf (starting version: {start_version})')
    
    df_silverRoads = (spk.readStream
                      .format("delta")
                      .option("readChangeFeed", "true")
                      .option("startingVersion", start_version)
                      .table(f"`{cfg.catalog}`.`silver`.`silver_roads_cdf`"))
    
    # Filter for inserts and updates
    df_silverRoads = (df_silverRoads
                      .filter(col("_change_type").isin(['insert', 'update_postimage']))
                      .drop('_change_type', '_commit_version', '_commit_timestamp'))
    
    print('✓ Reading silver_roads Success!')
    return df_silverRoads


def write_Traffic_to_Gold(StreamingDF, cfg, tracker):
    """Write to gold with metadata tracking"""
    print(f'Writing gold_traffic {cfg.catalog}.{cfg.schema}.gold_traffic_cdf...', end='')
    
    write_gold_traffic = (StreamingDF.writeStream
            .foreachBatch(
                lambda df, batch_id: tracker.write_batch(
                    df,
                    batch_id,
                    source_layer="silver",
                    source_table="silver_traffic_cdf",
                    target_layer="gold",
                    target_table="gold_traffic_cdf",
                    catalog=cfg.catalog,
                    schema=cfg.schema
                )
            )
            .option('checkpointLocation',cfg.checkpoint+ "/GoldTrafficLoadCDF/Checkpt/")
            .option("delta.enableChangeDataFeed", "true") 
            .queryName("GoldTrafficWriteCDFStream")
            .trigger(availableNow=True)
            .start())
    
    write_gold_traffic.awaitTermination()
    print('✓ Gold traffic_aggregates write complete!')

def write_Roads_to_Gold(StreamingDF,cfg, tracker):
    print(f'Writing gold_roads {cfg.catalog}.{cfg.schema}.gold_roads_cdf...', end='')

    write_gold_roads = (StreamingDF.writeStream
            .foreachBatch(
                lambda df, batch_id: tracker.write_batch(
                    df,
                    batch_id,
                    source_layer="silver",
                    source_table="silver_roads_cdf",
                    target_layer="gold",
                    target_table="gold_roads_cdf",
                    catalog=cfg.catalog,
                    schema=cfg.schema
                )
            )
            .option('checkpointLocation',cfg.checkpoint+ "/GoldRoadsLoadCDF/Checkpt/")
            .option("delta.enableChangeDataFeed", "true") 
            .queryName("GoldRoadsWriteCDFStream")
            .trigger(availableNow=True)
            .start())
    
    write_gold_roads.awaitTermination()
    print(f'Writing `{cfg.catalog}`.`{cfg.schema}`.`gold_roads_cdf` Success!')

def create_traffic_aggregates(df_traffic):
    """Create gold-level traffic aggregations"""
    df_gold = (df_traffic
                .withColumn('date', to_date(col("Count_date"), "yyyy-MM-dd HH:mm"))
                .withColumn('total_vehicles',
                            col('Electric_Vehicles_Count') + col('Two_wheeled_motor_vehicles') + col('Cars_and_taxis') + col('Buses_and_coaches')
                            )
                .groupBy('Road_Category_Id', 'region_name', 'date')
                .agg(
                   _sum('total_vehicles').alias('total_daily_vehicles'),
                   _avg('total_vehicles').alias('avg_hourly_vehicles'),
                   _count('*').alias('hourly_readings')
               ))
    
    return df_gold

def create_road_analytics(df_traffic, df_roads):
    """Join traffic and roads for gold analytics"""
    df_gold = (df_traffic.alias("traffic")
               .join(df_roads.alias("roads"), 'Road_Category_Id', 'inner')
               .groupBy('road_category', 'road_type', 'traffic.region_name')
               .agg(
                   _sum('total_vehicles').alias('total_vehicles_by_road_type'),
                   _avg('total_vehicles').alias('avg_vehicles_by_road_type'),
                   _count('*').alias('reading_count')
               )
               .withColumn('gold_processed_at', current_timestamp()))
    
    return df_gold


def write_Gold_RoadAnalytics(StreamingDF, cfg, tracker):
    """Write road analytics to gold with metadata tracking"""
    print('Writing gold_road_analytics...')
    
    write_Stream = (StreamingDF.writeStream
            .foreachBatch(
                lambda df, batch_id: tracker.write_batch(
                    df,
                    batch_id,
                    source_layer="silver",
                    source_table="silver_roads_cdf",
                    target_layer="gold",
                    target_table="gold_road_analytics_cdf",
                    catalog=cfg.catalog,
                    schema=cfg.schema
                )
            )
            .option('checkpointLocation', cfg.checkpoint + "/GoldRoadAnalyticsCDF/Checkpt/")
            .option("delta.enableChangeDataFeed", "true") 
            .queryName("GoldRoadAnalyticsWriteCDFStream")
            .trigger(availableNow=True)
            .start())
    
    write_Stream.awaitTermination()
    print('✓ Gold road_analytics write complete!')

def run_gold(env: str, tracker):
    spark = SparkSession.getActiveSession()
    cfg = get_config(env, "gold")
    
    print('\n=== Processing Gold Layer ===')
    
    # Get last processed versions
    last_traffic_version = tracker.get_last_version(
        source_layer='silver',
        source_table='silver_traffic_cdf',
        target_layer='gold',
        target_table='gold_traffic_cdf'
    )
    
    last_roads_version = tracker.get_last_version(
        source_layer='silver',
        source_table='silver_roads_cdf',
        target_layer='gold',
        target_table='gold_roads_cdf'
    )
    
    last_roads_analytics_version = tracker.get_last_version(
        source_layer='silver',
        source_table='silver_roads_cdf',
        target_layer='gold',
        target_table='gold_road_analytics_cdf'
    )
    # Read silver data with CDF
    df_traffic = read_SilverTraffic_CDF(spark, cfg, start_version=last_traffic_version)
    df_roads = read_SilverRoads_CDF(spark, cfg, start_version=last_roads_version)
    
    df_vehicle = safe_transform(df_traffic, create_VehicleIntensity, "create_VehicleIntensity")
    df_FinalRoads = create_LoadTime(df_roads)
    df_FinalTraffic = create_LoadTime(df_vehicle)
    # Create gold aggregates
    df_gold_traffic = create_traffic_aggregates(df_FinalTraffic)
    write_Roads_to_Gold(df_FinalRoads, cfg, tracker)
    write_Traffic_to_Gold(df_FinalTraffic, cfg, tracker)

    # Create gold analytics
    df_gold_analytics = create_road_analytics(df_gold_traffic, df_FinalRoads)
    write_Gold_RoadAnalytics(df_gold_analytics, cfg, tracker)
    
    print('\n=== Gold Processing Complete ===')

    run_id = os.getenv("DATABRICKS_JOB_RUN_ID", "local")
    print(f"Ingestion complete. run_id={run_id}")