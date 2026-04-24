# =========================
# main.py
# =========================
import sys
from pyspark.sql import SparkSession
from pipelines.bronze.load_to_bronze import run_bronze
from pipelines.silver.load_to_silver import run_silver
from pipelines.gold.load_to_gold import run_gold


if __name__ == "__main__":
    # Initialize or get existing Spark session
    spark = SparkSession.builder \
        .appName("Medallion Architecture Pipeline") \
        .getOrCreate()
    
    # Get the environment from command-line argument (passed from job parameter)
    # Default to 'dev' if not provided
    
    env_var = spark.conf.get("env", "dev")
    print(f"Running with environment: {env_var}")

    # Execute layers sequentially
    run_bronze(env_var)
    run_silver(env_var)
    run_gold(env_var)
    
    # Stop the Spark session
    spark.stop()



from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

PIPELINE_NAME = "cdf_pipeline_1"
SOURCE_TABLE = "source_table"
TARGET_TABLE = "target_table"

def get_last_processed_version():
    df = spark.sql(f"""
        SELECT last_processed_version
        FROM default.pipeline_metadata
        WHERE pipeline_name = '{PIPELINE_NAME}'
    """)
    
    result = df.collect()
    return result[0][0] if result else 0


def update_last_processed_version(version):
    spark.sql(f"""
        MERGE INTO pipeline_metadata t
        USING (SELECT '{PIPELINE_NAME}' AS pipeline_name, {version} AS version) s
        ON t.pipeline_name = s.pipeline_name
        WHEN MATCHED THEN UPDATE SET 
            last_processed_version = s.version,
            last_updated = current_timestamp()
        WHEN NOT MATCHED THEN INSERT *
    """)


def run_pipeline():
    try:
        # 1. Get last processed version
        last_version = get_last_processed_version()

        # 2. Read CDF incrementally
        updates_df = (
            spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", last_version)
            .table(SOURCE_TABLE)
        )

        # 3. Filter relevant changes
        updates_df = updates_df.filter(
            col("_change_type").isin("insert", "update_postimage", "delete")
        )

        if updates_df.isEmpty():
            print("No new data to process.")
            return

        # 4. Get max version processed (for checkpoint)
        max_version = updates_df.agg({"_commit_version": "max"}).collect()[0][0]

        # 5. Perform MERGE
        target = DeltaTable.forName(spark, TARGET_TABLE)

        (
            target.alias("t")
            .merge(
                updates_df.alias("s"),
                "t.id = s.id"
            )
            .whenMatchedUpdateAll(condition="s._change_type != 'delete'")
            .whenMatchedDelete(condition="s._change_type = 'delete'")
            .whenNotMatchedInsertAll(condition="s._change_type != 'delete'")
            .execute()
        )

        # 6. Update metadata ONLY after successful merge
        update_last_processed_version(max_version)

        print(f"Pipeline completed successfully. Version updated to {max_version}")

    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        raise e  # Let orchestration tool (Airflow, Jobs) handle retry