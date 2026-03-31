# =========================
# common/transformations.py
# =========================
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp


def remove_Dups(df):
    print('Removing Duplicate values: ',end='')
    df_dup = df.dropDuplicates()
    print('Success!')
    print('***********************')
    return df_dup



def handle_NULLs(df,Columns):
    print('Replacing NULLs of Strings DataType with "Unknown": ', end='')
    df_string = df.fillna('Unknown',subset=Columns)
    print('Success!')
    print('Replacing NULLs of Numeric DataType with "0":  ', end='')
    df_numeric = df_string.fillna(0,subset=Columns)
    print('Success!')
    print('***********************')
    return df_numeric

def ev_Count(df):
    print('Creating Electric Vehicles Count Column : ', end='')
    from pyspark.sql.functions import col
    df_ev = df.withColumn('Electric_Vehicles_Count',
                            col('EV_Car') + col('EV_Bike')
                            )
    
    print('Success!! ')
    return df_ev

def Motor_Count(df):
    print('Creating All Motor Vehicles Count Column : ', end='')
    from pyspark.sql.functions import col
    df_motor = df.withColumn('Motor_Vehicles_Count',
                            col('Electric_Vehicles_Count') + col('Two_wheeled_motor_vehicles') + col('Cars_and_taxis') + col('Buses_and_coaches') + col('LGV_Type') + col('HGV_Type')
                            )
    
    print('Success!! ')
    return df_motor

def create_TransformedTime(df):
    from pyspark.sql.functions import current_timestamp
    print('Creating Transformed Time column : ',end='')
    df_timestamp = df.withColumn('Transformed_Time',
                      current_timestamp()
                      )
    print('Success!!')
    return df_timestamp

def create_VehicleIntensity(df):
 from pyspark.sql.functions import col
 print('Creating Vehicle Intensity column : ',end='')
 df_veh = df.withColumn('Vehicle_Intensity',
               col('Motor_Vehicles_Count') / col('Link_length_km')
               )
 print("Success!!!")
 print('***************')
 return 

def create_LoadTime(df):
    from pyspark.sql.functions import current_timestamp
    print('Creating Load Time column : ',end='')
    df_timestamp = df.withColumn('Load_Time',
                      current_timestamp()
                      )
    print('Success!!')
    print('**************')
    return df_timestamp

def road_Category(df):
    print('Creating Road Category Name Column: ', end='')
    from pyspark.sql.functions import when,col

    df_road_Cat = df.withColumn("Road_Category_Name",
                  when(col('Road_Category') == 'TA', 'Class A Trunk Road')
                  .when(col('Road_Category') == 'TM', 'Class A Trunk Motor')
                   .when(col('Road_Category') == 'PA','Class A Principal road')
                    .when(col('Road_Category') == 'PM','Class A Principal Motorway')
                    .when(col('Road_Category') == 'M','Class B road')
                    .otherwise('NA')
                  
                  )
    print('Success!! ')
    print('***********************')
    return df_road_Cat

def road_Type(df):
    print('Creating Road Type Name Column: ', end='')
    from pyspark.sql.functions import when,col

    df_road_Type = df.withColumn("Road_Type",
                  when(col('Road_Category_Name').like('%Class A%'),'Major')
                  .when(col('Road_Category_Name').like('%Class B%'),'Minor')
                    .otherwise('NA')
                  
                  )
    print('Success!! ')
    print('***********************')
    return df_road_Type