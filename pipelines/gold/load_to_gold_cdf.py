# =========================
# pipelines/gold/gold_orders.py (with metadata tracking)
# =========================
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, sum as _sum, avg as _avg, count as _count
from config.config import get_config
from common.transformations import create_LoadTime, create_VehicleIntensity


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


def write_Gold_Traffic(StreamingDF, cfg, tracker):
    """Write to gold with metadata tracking"""
    print('Writing gold_traffic')
    
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
            .option('checkpointLocation',cfg.checkpoint+ "GoldTrafficLoadCDF/Checkpt/")
            .option("delta.enableChangeDataFeed", "true") 
            .queryName("GoldTrafficWriteCDFStream")
            .trigger(availableNow=True)
            .start())
    
    write_gold_traffic.awaitTermination()
    print('✓ Gold traffic_aggregates write complete!')

def write_Roads_to_Gold(StreamingDF,cfg, tracker):
    print('Writing the gold_roads Data : ',end='') 

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
                .option('checkpointLocation',cfg.checkpoint+ "GoldRoadsLoadCDF/Checkpt/")
                .option("delta.enableChangeDataFeed", "true") 
                .queryName("GoldRoadsWriteCDFStream")
                .trigger(availableNow=True)
                .toTable(f"`{cfg.catalog}`.`{cfg.schema}`.`gold_roads`"))
    
    write_gold_roads.awaitTermination()
    print(f'Writing `{cfg.catalog}`.`{cfg.schema}`.`gold_roads` Success!')

def create_traffic_aggregates(df_traffic):
    """Create gold-level traffic aggregations"""
    df_gold = (df_traffic
               .groupBy('road_id', 'date')
               .agg(
                   _sum('total_vehicles').alias('total_daily_vehicles'),
                   _avg('total_vehicles').alias('avg_hourly_vehicles'),
                   _sum('ev_count').alias('total_ev_count'),
                   _sum('motor_count').alias('total_motor_count'),
                   _count('*').alias('hourly_readings')
               )
               .withColumn('gold_processed_at', current_timestamp()))
    
    return df_gold

def create_road_analytics(df_traffic, df_roads):
    """Join traffic and roads for gold analytics"""
    df_gold = (df_traffic
               .join(df_roads, 'road_id', 'inner')
               .groupBy('road_category', 'road_type', 'region')
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
                    target_table="road_analytics",
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
        target_table='road_analytics'
    )
    # Read silver data with CDF
    df_traffic = read_SilverTraffic_CDF(spark, cfg, start_version=last_traffic_version)
    df_roads = read_SilverRoads_CDF(spark, cfg, start_version=last_roads_version)

    df_vehicle = create_VehicleIntensity(df_traffic)
    df_FinalTraffic = create_LoadTime(df_vehicle)
    df_FinalRoads = create_LoadTime(df_roads)

    # Create gold aggregates
    #df_gold_traffic = create_traffic_aggregates(df_FinalTraffic)
    write_Gold_Traffic(df_FinalTraffic, cfg, tracker)
    write_Roads_to_Gold(df_FinalRoads, cfg, tracker)
    
    # Create gold analytics
    df_gold_analytics = create_road_analytics(df_FinalTraffic, df_FinalRoads)
    write_Gold_RoadAnalytics(df_gold_analytics, cfg, tracker)
    
    print('\n=== Gold Processing Complete ===')

    run_id = os.getenv("DATABRICKS_JOB_RUN_ID", "local")
    print(f"Ingestion complete. run_id={run_id}")