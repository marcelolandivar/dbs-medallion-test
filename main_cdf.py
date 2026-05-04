# =========================
# main.py
# =========================
import argparse
from pyspark.sql import SparkSession
from pipelines.bronze.load_to_bronze_cdf import run_bronze
from pipelines.silver.load_to_silver_cdf import run_silver
from pipelines.gold.load_to_gold_cdf import run_gold
from pipelines.utils.metadata_tracker import MetadataTracker

if __name__ == "__main__":
    # Initialize or get existing Spark session
    spark = SparkSession.getActiveSession()

    # Get the environment from command-line argument (passed from job parameter)
    # Default to 'dev' if not provided
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default="dev")

    args = parser.parse_args()
    env_var = args.env
    print(f"Running with environment: {env_var}")


   # Initialize metadata tracker
    tracker = MetadataTracker(spark, catalog=f"{env_var}_catalog" , schema="metadata")

    # Execute layers sequentially
    run_bronze(env_var, tracker)
    run_silver(env_var, tracker)
    run_gold(env_var, tracker)

