

# =========================
# pipelines/bronze/load_to_bronze.py
# =========================
from pyspark.sql import SparkSession
from config.config import get_config
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp
import os


def read_Traffic_Data(spk, cfg):

    print("Reading the Raw Traffic Data :  ", end='')

    schema = StructType([
        StructField("Record_ID",IntegerType()),
        StructField("Count_point_id",IntegerType()),
        StructField("Direction_of_travel",StringType()),
        StructField("Year",IntegerType()),
        StructField("Count_date",StringType()),
        StructField("hour",IntegerType()),
        StructField("Region_id",IntegerType()),
        StructField("Region_name",StringType()),
        StructField("Local_authority_name",StringType()),
        StructField("Road_name",StringType()),
        StructField("Road_Category_ID",IntegerType()),
        StructField("Start_junction_road_name",StringType()),
        StructField("End_junction_road_name",StringType()),
        StructField("Latitude",DoubleType()),
        StructField("Longitude",DoubleType()),
        StructField("Link_length_km",DoubleType()),
        StructField("Pedal_cycles",IntegerType()),
        StructField("Two_wheeled_motor_vehicles",IntegerType()),
        StructField("Cars_and_taxis",IntegerType()),
        StructField("Buses_and_coaches",IntegerType()),
        StructField("LGV_Type",IntegerType()),
        StructField("HGV_Type",IntegerType()),
        StructField("EV_Car",IntegerType()),
        StructField("EV_Bike",IntegerType()),
        StructField('Extract_Time',TimestampType())
    ])

    rawTraffic_stream = (spk.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{cfg.checkpoint}/rawTrafficLoad/schemaInfer')
        .option('header','true')
        .option("ignoreDeletes", "true")  
        .schema(schema)
        .load(cfg.landing+'/raw_traffic/')
        .withColumn("Extract_Time", current_timestamp()))

    
    print('Reading Succcess !!')
    print('*******************')

    return rawTraffic_stream

def read_Road_Data(spk, cfg):
 
    print("Reading the Raw Roads Data :  ", end='')
    schema = StructType([
        StructField('Road_ID',IntegerType()),
        StructField('Road_Category_Id',IntegerType()),
        StructField('Road_Category',StringType()),
        StructField('Region_ID',IntegerType()),
        StructField('Region_Name',StringType()),
        StructField('Total_Link_Length_Km',DoubleType()),
        StructField('Total_Link_Length_Miles',DoubleType()),
        StructField('All_Motor_Vehicles',DoubleType()),
        StructField('Extract_Time',TimestampType())
        ])

    rawRoads_stream = (spk.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option('cloudFiles.schemaLocation',f'{cfg.checkpoint}/rawRoadsLoad/schemaInfer')
        .option('header','true')
        .option("ignoreDeletes", "true")  
        .schema(schema)
        .load(cfg.landing+'/raw_roads/')
        .withColumn("Extract_Time", current_timestamp())
        )
    
    print('Reading Succcess !!')
    print('*******************')

    return rawRoads_stream

def write_Traffic_Data(StreamingDF, cfg):
    print(f'Writing data to {cfg.catalog} raw_traffic table', end='' )
    write_Stream = (StreamingDF.writeStream
                    .format('delta')
                    .option("checkpointLocation",cfg.checkpoint + '/rawTrafficLoad/Checkpt')
                    .outputMode('append')
                    .queryName('rawTrafficWriteStream')
                    .trigger(availableNow=True)
                    .toTable(f"`{cfg.catalog}`.`{cfg.schema}`.`raw_traffic`"))
    
    write_Stream.awaitTermination()
    print('Write Success')
    print("****************************")    

def write_Road_Data(StreamingDF, cfg):
    print(f'Writing data to {cfg.catalog} raw_roads table', end='' )
    write_Data = (StreamingDF.writeStream
                    .format('delta')
                    .option("checkpointLocation",cfg.checkpoint + '/rawRoadsLoad/Checkpt')
                    .outputMode('append')
                    .queryName('rawRoadsWriteStream')
                    .trigger(availableNow=True)
                    .toTable(f"`{cfg.catalog}`.`{cfg.schema}`.`raw_roads`"))
    
    write_Data.awaitTermination()
    print('Write Success')
    print("****************************")    

def run_bronze(env: str):

    spark = SparkSession.getActiveSession()
    cfg = get_config(env, "bronze")

    rawTraffic_stream = read_Traffic_Data(spark, cfg)
    rawRoads_stream = read_Road_Data(spark, cfg)

    write_Traffic_Data(rawTraffic_stream, cfg)
    write_Road_Data(rawRoads_stream, cfg)
    run_id = os.getenv("DATABRICKS_JOB_RUN_ID", "local")
    print(f"Ingestion complete. run_id={run_id}")


