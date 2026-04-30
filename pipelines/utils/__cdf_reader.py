# =========================
# utils/cdf_reader.py
# =========================
from pyspark.sql import SparkSession

class CDFReader:
    def __init__(self, spark, tracker):
        self.spark = spark
        self.tracker = tracker
    
    def read_changes(self, table_name, layer, target_layer):
        """Read CDF changes since last processed version"""
        
        # Get last processed version from metadata
        last_version = self.tracker.get_last_version(target_layer, table_name)
        
        if last_version == 0:
            print(f"No previous version found. Reading full table: {table_name}")
            # First time - read entire table
            df = self.spark.read.format("delta").table(table_name)
            current_version = self.spark.sql(
                f"DESCRIBE HISTORY {table_name} LIMIT 1"
            ).collect()[0]['version']
        else:
            print(f"Reading CDF from version {last_version + 1} for {table_name}")
            # Read only changes since last processed version
            df = (self.spark.read
                  .format("delta")
                  .option("readChangeFeed", "true")
                  .option("startingVersion", last_version + 1)
                  .table(table_name))
            
            # Filter to get only current state (optional - exclude deletes)
            df = df.filter(df._change_type.isin(['insert', 'update_postimage']))
            df = df.drop('_change_type', '_commit_version', '_commit_timestamp')
            
            current_version = df.agg({"_commit_version": "max"}).collect()[0][0]
        
        return df, current_version