# =========================
# pipelines/silver/silver_orders.py
# =========================
from pyspark.sql import SparkSession
from config.config import get_config
from common.transformations import Motor_Count, create_TransformedTime, ev_Count, handle_NULLs, normalize_orders, remove_Dups, road_Category, road_Type
from common.validations import validate_not_null, drop_duplicates


def read_BronzeTrafficTable(spk, cfg):
    print('Reading the Bronze Table Data : ',end='')
    df_bronzeTraffic = (spk.readStream
                    .table(f"`{cfg.catalog}`.`{cfg.schema}`.`raw_traffic`")
                    )
    print(f'Reading {cfg.catalog}.{cfg.schema}.raw_traffic Success!')
    return df_bronzeTraffic

def write_Traffic_to_Silver(StreamingDF,cfg):
    print('Writing the silver_traffic Data : ',end='') 

    write_StreamSilver = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',cfg.checkpoint+ "/SilverTrafficLoad/Checkpt/")
                .outputMode('append')
                .queryName("SilverTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{cfg.catalog}`.`{cfg.schema}`.`silver_traffic`"))
    
    write_StreamSilver.awaitTermination()
    print(f'Writing `{cfg.catalog}`.`{cfg.schema}`.`silver_traffic` Success!')

def read_BronzeRoadsTable(spk, cfg):
    print('Reading the Bronze Table raw_roads Data : ',end='')
    df_bronzeRoads = (spk.readStream
                    .table(f"`{cfg.catalog}`.`{cfg.schema}`.`raw_roads`")
                    )
    print(f'Reading {cfg.catalog}.{cfg.schema}.raw_roads Success!')
    print("**********************************")
    return df_bronzeRoads

def write_Roads_to_Silver(StreamingDF,cfg):
    print('Writing the silver_roads Data : ',end='') 

    write_StreamSilver_R = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',cfg.checkpoint+ "/SilverRoadsLoad/Checkpt/")
                .outputMode('append')
                .queryName("SilverRoadsWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{cfg.catalog}`.`{cfg.schema}`.`silver_roads`"))
    
    write_StreamSilver_R.awaitTermination()
    print(f'Writing `{cfg.catalog}`.`{cfg.schema}`.`silver_roads` Success!')

def run_silver(env: str):
    spark = SparkSession.getActiveSession()
    cfg = get_config(env, "silver")

    #### Traffic Data ####
    print('Processing Traffic Data : ',end='')
    df_traffic_data = read_BronzeTrafficTable(spark, cfg)
    df_traffic_data = remove_Dups(df_traffic_data)
    all_columns_traffic = df_traffic_data.schema.names
    df_traffic_data = handle_NULLs(df_traffic_data, all_columns_traffic)
    df_traffic_data = ev_Count(df_traffic_data)
    df_traffic_data = Motor_Count(df_traffic_data)
    df_traffic_data = create_TransformedTime(df_traffic_data)

    write_Traffic_to_Silver(df_traffic_data, cfg)

    #### Roads Data ####
    print('Processing Roads Data : ',end='')
    read_BronzeRoadsTable(spark, cfg)
    df_roads_data = read_BronzeRoadsTable(spark, cfg)
    df_roads_data = remove_Dups(df_roads_data)
    all_columns_roads = df_roads_data.schema.names
    df_roads_data = handle_NULLs(df_roads_data,all_columns_roads)
    df_roads_data = road_Category(df_roads_data)
    df_roads_data = road_Type(df_roads_data)

    write_Roads_to_Silver(df_roads_data, cfg)
