# =========================
# pipelines/gold/gold_sales.py
# =========================
from pyspark.sql import SparkSession
from config.config import get_config
from common.transformations import create_LoadTime, create_VehicleIntensity
import os


def read_SilverTrafficTable(spk, cfg):
    print('Reading the Silver Traffic Table Data : ',end='')
    df_SilverTraffic = (spk.readStream
                    .table(f"`{cfg.catalog}`.`silver`.`silver_traffic`")
                    )
    print(f'Reading {cfg.catalog}.silver.silver_traffic Success!')
    print("**********************************")
    return df_SilverTraffic

def read_SilverRoadsTable(spk, cfg):
    print('Reading the Silver Table Silver_roads Data : ',end='')
    df_SilverRoads = (spk.readStream
                    .table(f"`{cfg.catalog}`.`silver`.`silver_roads`")
                    )
    print(f'Reading {cfg.catalog}.silver.silver_roads Success!')
    print("**********************************")
    return df_SilverRoads

def write_Traffic_to_Gold(StreamingDF,cfg):
    print('Writing the gold_traffic Data : ',end='') 

    write_gold_traffic = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',cfg.checkpoint+ "GoldTrafficLoad/Checkpt/")
                .outputMode('append')
                .queryName("GoldTrafficWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{cfg.catalog}`.`{cfg.schema}`.`gold_traffic`"))
    
    write_gold_traffic.awaitTermination()
    print(f'Writing `{cfg.catalog}`.`{cfg.schema}`.`gold_traffic` Success!')

def write_Roads_to_Gold(StreamingDF,cfg):
    print('Writing the gold_roads Data : ',end='') 

    write_gold_roads = (StreamingDF.writeStream
                .format('delta')
                .option('checkpointLocation',cfg.checkpoint+ "GoldRoadsLoad/Checkpt/")
                .outputMode('append')
                .queryName("GoldRoadsWriteStream")
                .trigger(availableNow=True)
                .toTable(f"`{cfg.catalog}`.`{cfg.schema}`.`gold_roads`"))
    
    write_gold_roads.awaitTermination()
    print(f'Writing `{cfg.catalog}`.`{cfg.schema}`.`gold_roads` Success!')

def run_gold(env: str):
    spark = SparkSession.getActiveSession()
    cfg = get_config(env, "gold")

    df_traffic = read_SilverTrafficTable(spark, cfg)
    df_roads = read_SilverRoadsTable(spark, cfg)
    df_traffic.show()
    df_vehicle = create_VehicleIntensity(df_traffic)
    df_FinalTraffic = create_LoadTime(df_vehicle)
    df_FinalRoads = create_LoadTime(df_roads)

    write_Traffic_to_Gold(df_FinalTraffic, cfg)
    write_Roads_to_Gold(df_FinalRoads, cfg)

    run_id = os.getenv("DATABRICKS_JOB_RUN_ID", "local")
    print(f"Ingestion complete. run_id={run_id}")