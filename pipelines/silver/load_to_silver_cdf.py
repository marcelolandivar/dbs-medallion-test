# =========================
# pipelines/silver/silver_orders.py (with metadata tracking)
# =========================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from config.config import get_config
from common.transformations import Motor_Count, create_TransformedTime, ev_Count, handle_NULLs, remove_Dups, road_Category, road_Type
from common.validations import validate_not_null, drop_duplicates


def read_BronzeTrafficTable_CDF(spk, cfg, start_version=0):
    """Read Change Data Feed from bronze traffic table"""
    print(f'Reading CDF from {cfg.catalog}.bronze.raw_traffic (starting version: {start_version})')
    
    df_bronzeTraffic = (spk.readStream
                        .format("delta")
                        .option("readChangeFeed", "true")
                        .option("startingVersion", start_version)
                        .table(f"`{cfg.catalog}`.`bronze`.`raw_traffic`"))
    
    # Filter for inserts and updates only (exclude deletes)
    df_bronzeTraffic = (df_bronzeTraffic
                        .filter(col("_change_type").isin(['insert', 'update_postimage']))
                        .drop('_change_type', '_commit_version', '_commit_timestamp'))
    
    print(f'✓ Reading {cfg.catalog}.bronze.raw_traffic Success!')
    return df_bronzeTraffic


def write_Traffic_to_Silver(StreamingDF, cfg, tracker):
    """Write traffic data to silver with metadata tracking"""
    print('Writing the silver_traffic Data...') 

    write_StreamSilver = (StreamingDF.writeStream
            .foreachBatch(
                lambda df, batch_id: tracker.write_batch(
                    df,
                    batch_id,
                    source_layer="bronze",
                    source_table="roads",
                    target_layer="silver",
                    target_table="silver_roads"
                )
            )
            .option('checkpointLocation', cfg.checkpoint + "/SilverRoadsLoad/Checkpt/")
            .queryName("SilverRoadsWriteStream")
            .trigger(availableNow=True)
            .start())
    
    write_StreamSilver.awaitTermination()
    print(f'✓ Writing `{cfg.catalog}`.`{cfg.schema}`.`silver_traffic` Success!')


def read_BronzeRoadsTable_CDF(spk, cfg, start_version=0):
    """Read Change Data Feed from bronze roads table"""
    print(f'Reading CDF from {cfg.catalog}.bronze.raw_roads (starting version: {start_version})')
    
    df_bronzeRoads = (spk.readStream
                      .format("delta")
                      .option("readChangeFeed", "true")
                      .option("startingVersion", start_version)
                      .table(f"`{cfg.catalog}`.`bronze`.`raw_roads`"))
    
    # Filter for inserts and updates only
    df_bronzeRoads = (df_bronzeRoads
                      .filter(col("_change_type").isin(['insert', 'update_postimage']))
                      .drop('_change_type', '_commit_version', '_commit_timestamp'))
    
    print(f'✓ Reading {cfg.catalog}.bronze.raw_roads Success!')
    return df_bronzeRoads


def write_Roads_to_Silver(StreamingDF, cfg, tracker):
    """Write roads data to silver with metadata tracking"""
    print('Writing the silver_roads Data...') 

    write_StreamSilver_R = (
        StreamingDF.writeStream
            .foreachBatch(
                lambda df, batch_id: tracker.write_batch(
                    df,
                    batch_id,
                    source_layer="bronze",
                    source_table="roads",
                    target_layer="silver",
                    target_table="silver_roads"
                )
            )
            .option('checkpointLocation', cfg.checkpoint + "/SilverRoadsLoad/Checkpt/")
            .queryName("SilverRoadsWriteStream")
            .trigger(availableNow=True)
            .start())
    
    write_StreamSilver_R.awaitTermination()
    print(f'✓ Writing `{cfg.catalog}`.`{cfg.schema}`.`silver_roads` Success!')


def run_silver(env: str, tracker):
    spark = SparkSession.getActiveSession()
    cfg = get_config(env, "silver")

    #### Traffic Data ####
    print('\n=== Processing Traffic Data ===')
    
    # Get last processed version
    last_traffic_version = tracker.get_last_version(
        source_layer='bronze',
        source_table='raw_traffic',
        target_layer='silver',
        target_table='silver_traffic'
    )
    
    # Read CDF from last processed version
    df_traffic_data = read_BronzeTrafficTable_CDF(spark, cfg, start_version=last_traffic_version)
    df_traffic_data = remove_Dups(df_traffic_data)
    all_columns_traffic = df_traffic_data.schema.names
    df_traffic_data = handle_NULLs(df_traffic_data, all_columns_traffic)
    df_traffic_data = ev_Count(df_traffic_data)
    df_traffic_data = Motor_Count(df_traffic_data)
    df_traffic_data = create_TransformedTime(df_traffic_data)
    df_traffic_data = df_traffic_data.withColumn('silver_processed_at', current_timestamp())

    write_Traffic_to_Silver(df_traffic_data, cfg, tracker)

    #### Roads Data ####
    print('\n=== Processing Roads Data ===')
    
    # Get last processed version
    last_roads_version = tracker.get_last_version(
        source_layer='bronze',
        source_table='raw_roads',
        target_layer='silver',
        target_table='silver_roads'
    )
    
    # Read CDF from last processed version
    df_roads_data = read_BronzeRoadsTable_CDF(spark, cfg, start_version=last_roads_version)
    df_roads_data = remove_Dups(df_roads_data)
    all_columns_roads = df_roads_data.schema.names
    df_roads_data = handle_NULLs(df_roads_data, all_columns_roads)
    df_roads_data = road_Category(df_roads_data)
    df_roads_data = road_Type(df_roads_data)
    df_roads_data = df_roads_data.withColumn('silver_processed_at', current_timestamp())

    write_Roads_to_Silver(df_roads_data, cfg, tracker)
    
    print('\n=== Silver Processing Complete ===')