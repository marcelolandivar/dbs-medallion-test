# =========================
# utils/metadata_tracker.py
# =========================
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from datetime import datetime

class MetadataTracker:
    def __init__(self, spark, catalog="default", schema="metadata"):
        self.spark = spark
        self.metadata_table = f"{catalog}.{schema}.pipeline_metadata"
        self._initialize_table()
    
    def _initialize_table(self):
        """Create metadata table if it doesn't exist"""
        self.spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {self.metadata_table.rsplit('.', 1)[0]}
        """)
        
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {self.metadata_table} (
            source_layer STRING,
            source_table STRING,
            target_layer STRING,
            target_table STRING,
            last_processed_version BIGINT,
            last_processed_timestamp TIMESTAMP,
            record_count BIGINT,
            status STRING,
            updated_at TIMESTAMP
        )
        USING DELTA
        """)
        print(f"Metadata table initialized: {self.metadata_table}")
    
    def get_last_version(self, source_layer, source_table, target_layer, target_table):
        """Get the last successfully processed version"""
        try:
            result = self.spark.sql(f"""
                SELECT last_processed_version 
                FROM {self.metadata_table}
                WHERE source_layer = '{source_layer}' 
                  AND source_table = '{source_table}'
                  AND target_layer = '{target_layer}'
                  AND target_table = '{target_table}'
                  AND status = 'SUCCESS'
                ORDER BY updated_at DESC
                LIMIT 1
            """).collect()
            
            if result:
                version = result[0]['last_processed_version']
                print(f"Last processed version for {source_table} -> {target_table}: {version}")
                return version
            else:
                print(f"No previous version found for {source_table} -> {target_table}. Starting from 0")
                return 0
        except Exception as e:
            print(f"Error getting last version: {e}. Starting from 0")
            return 0
    
    def update_version(self, source_layer, source_table, target_layer, target_table, 
                      version, record_count=0, status='SUCCESS'):
        """Update the last processed version"""
        try:
            update_df = self.spark.createDataFrame([{
                'source_layer': source_layer,
                'source_table': source_table,
                'target_layer': target_layer,
                'target_table': target_table,
                'last_processed_version': version,
                'last_processed_timestamp': datetime.now(),
                'record_count': record_count,
                'status': status,
                'updated_at': datetime.now()
            }])
            
            update_df.write.format("delta").mode("append").saveAsTable(self.metadata_table)
            print(f"✓ Updated metadata: {source_table} -> {target_table} | Version: {version} | Records: {record_count}")
        except Exception as e:
            print(f"✗ Error updating metadata: {e}")
    
    @classmethod
    def write_batch(self, batch_df, batch_id, source_layer, source_table, target_layer, target_table):
        if batch_df.count() > 0:
            # Get current version of source table
            source_version = self.get_last_version(source_layer, source_table, target_layer, target_table)

            # Write to silver
            record_count = batch_df.count()
            batch_df.write.format("delta").mode("append").saveAsTable(
                f"`{self.catalog}`.`{target_layer}`.`{target_table}`"
            )
            
            # Update metadata
            self.update_version(
                source_layer=source_layer,
                source_table=source_table,
                target_layer=target_layer,
                target_table=target_table,
                version=source_version,
                record_count=record_count,
                status='SUCCESS'
            )
    