from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def get_traffic_schema():
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
            StructField("EV_Bike",IntegerType())
        ])
    return schema
