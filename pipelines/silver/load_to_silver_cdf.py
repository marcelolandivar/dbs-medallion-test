# =========================
# pipelines/silver/silver_orders.py (with metadata tracking)
# =========================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from config.config import get_config
from common.transformations import Motor_Count, create_TransformedTime, ev_Count, handle_NULLs, remove_Dups, road_Category, road_Type
from common.validations import validate_not_null, drop_duplicates, safe_transform


def read_BronzeTrafficTable_CDF(spk, cfg, start_version=0):
    """Read Change Data Feed from bronze traffic table"""
    print(f'Reading CDF from {cfg.catalog}.bronze.raw_traffic_cdf (starting version: {start_version})')
    
    df_bronzeTraffic = (spk.readStream
                        .format("delta")
                        .option("readChangeFeed", "true")
                        .option("startingVersion", start_version)
                        .table(f"`{cfg.catalog}`.`bronze`.`raw_traffic_cdf`"))
    
    # Filter for inserts and updates only (exclude deletes)
    df_bronzeTraffic = (df_bronzeTraffic
                        .filter(col("_change_type").isin(['insert', 'update_postimage']))
                        .drop('_change_type', '_commit_version', '_commit_timestamp'))
    
    print(f'✓ Reading {cfg.catalog}.bronze.raw_traffic_cdf Success!')
    return df_bronzeTraffic


def write_Traffic_to_Silver(StreamingDF, cfg, tracker):
    """Write traffic data to silver with metadata tracking"""
    print('Writing the silver_traffic Data CDF...') 

    write_StreamSilver = (StreamingDF.writeStream
            .foreachBatch(
                lambda df, batch_id: tracker.write_batch(
                    df,
                    batch_id,
                    source_layer="bronze",
                    source_table="raw_traffic_cdf",
                    target_layer="silver",
                    target_table="silver_traffic_cdf",
                    catalog=cfg.catalog,  
                    schema=cfg.schema  
                )
            )
            .option('checkpointLocation', cfg.checkpoint + "/SilverTrafficLoadCDF/Checkpt/")
            .option("delta.enableChangeDataFeed", "true") 
            .option("delta.mergeSchema", "true")
            .queryName("SilverTrafficCDFWriteStream")
            .trigger(availableNow=True)
            .start())
    
    write_StreamSilver.awaitTermination()
    print(f'✓ Writing `{cfg.catalog}`.`{cfg.schema}`.`silver_traffic_cdf` Success!')


def read_BronzeRoadsTable_CDF(spk, cfg, start_version=0):
    """Read Change Data Feed from bronze roads table"""
    print(f'Reading CDF from {cfg.catalog}.bronze.raw_roads_cdf (starting version: {start_version})')
    
    df_bronzeRoads = (spk.readStream
                      .format("delta")
                      .option("readChangeFeed", "true")
                      .option("startingVersion", start_version)
                      .table(f"`{cfg.catalog}`.`bronze`.`raw_roads_cdf`"))
    
    # Filter for inserts and updates only
    df_bronzeRoads = (df_bronzeRoads
                      .filter(col("_change_type").isin(['insert', 'update_postimage']))
                      .drop('_change_type', '_commit_version', '_commit_timestamp'))
    
    print(f'✓ Reading {cfg.catalog}.bronze.raw_roads_cdf Success!')
    return df_bronzeRoads


def write_Roads_to_Silver(StreamingDF, cfg, tracker):
    """Write roads data to silver with metadata tracking"""
    print('Writing the silver_roads Data CDF...') 

    write_StreamSilver_R = (
        StreamingDF.writeStream
            .foreachBatch(
                lambda df, batch_id: tracker.write_batch(
                    df,
                    batch_id,
                    source_layer="bronze",
                    source_table="raw_roads_cdf",
                    target_layer="silver",
                    target_table="silver_roads_cdf",
                    catalog=cfg.catalog,  
                    schema=cfg.schema  
                )
            )
            .option('checkpointLocation', cfg.checkpoint + "/SilverRoadsLoadCDF/Checkpt/")
            .option("delta.enableChangeDataFeed", "true") 
            .option("delta.mergeSchema", "true")
            .queryName("SilverRoadsWriteCDFStream")
            .trigger(availableNow=True)
            .start())
    
    write_StreamSilver_R.awaitTermination()
    print(f'✓ Writing `{cfg.catalog}`.`{cfg.schema}`.`silver_roads_cdf` Success!')

def run_silver(env: str, tracker):
    spark = SparkSession.getActiveSession()
    cfg = get_config(env, "silver")

    #### Traffic Data ####
    print('\n=== Processing Traffic Data ===')
    
    # Get last processed version
    last_traffic_version = tracker.get_last_version(
        source_layer='bronze',
        source_table='raw_traffic_cdf',
        target_layer='silver',
        target_table='silver_traffic_cdf'
    )
    
    # Read CDF from last processed version
    df_traffic_data = read_BronzeTrafficTable_CDF(spark, cfg, start_version=last_traffic_version)


    # Then in run_silver:
    df_traffic_data = safe_transform(df_traffic_data, remove_Dups, "remove_Dups")
    df_traffic_data = safe_transform(
        df_traffic_data,  # First argument: the DataFrame
        lambda x: handle_NULLs(x, x.schema.names),  # Second: transform function
        "handle_NULLs"  # Third: name
    )
    df_traffic_data = safe_transform(df_traffic_data, ev_Count, "ev_Count")
    df_traffic_data = safe_transform(df_traffic_data, Motor_Count, "Motor_Count")
    df_traffic_data = create_TransformedTime(df_traffic_data)
    df_traffic_data = df_traffic_data.withColumn('silver_processed_at', current_timestamp())

    write_Traffic_to_Silver(df_traffic_data, cfg, tracker)

    #### Roads Data ####
    print('\n=== Processing Roads Data ===')
    
    # Get last processed version
    last_roads_version = tracker.get_last_version(
        source_layer='bronze',
        source_table='raw_roads_cdf',
        target_layer='silver',
        target_table='silver_roads_cdf'
    )
    
    # Read CDF from last processed version
    df_roads_data = read_BronzeRoadsTable_CDF(spark, cfg, start_version=last_roads_version)
    df_roads_data = safe_transform(df_roads_data, remove_Dups, "remove_Dups")
    df_traffic_data = safe_transform(
        df_roads_data,  
        lambda x: handle_NULLs(x, x.schema.names),  
        "handle_NULLs" 
    )
    df_roads_data = safe_transform(df_roads_data, road_Category, "road_Category")
    df_roads_data = safe_transform(df_roads_data, road_Type, "road_Type")
    df_roads_data = df_roads_data.withColumn('silver_processed_at', current_timestamp())

    write_Roads_to_Silver(df_roads_data, cfg, tracker)
    
    print('\n=== Silver Processing Complete ===')