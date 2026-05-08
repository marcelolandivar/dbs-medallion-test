# =========================
# main_cdf.py
# =========================
import argparse
import sys
from pyspark.sql import SparkSession
from pipelines.bronze.load_to_bronze_cdf import run_bronze
from pipelines.silver.load_to_silver_cdf import run_silver
from pipelines.gold.load_to_gold_cdf import run_gold
from pipelines.utils.metadata_tracker import MetadataTracker

if __name__ == "__main__":
    try:
        # Initialize or get existing Spark session
        spark = SparkSession.getActiveSession()
        
        if spark is None:
            print("ERROR: No active Spark session found. Ensure you're running in Databricks.")
            sys.exit(1)

        # Get the environment from command-line argument (passed from job parameter)
        # Default to 'dev' if not provided
        parser = argparse.ArgumentParser()
        parser.add_argument("--env", default="dev")#

        args = parser.parse_args()
        env_var = args.env
        print(f"Running with environment: {env_var}")

        # Initialize metadata tracker
        tracker = MetadataTracker(spark, catalog=f"{env_var}_catalog" , schema="metadata")

        # Execute layers sequentially
        print("\n=== Starting Pipeline Execution ===\n")
        run_bronze(env_var, tracker)
        print("\n✓ Bronze layer completed successfully\n")
        
        run_silver(env_var, tracker)
        print("\n✓ Silver layer completed successfully\n")
        
        run_gold(env_var, tracker)
        print("\n✓ Gold layer completed successfully\n")
        
        print("=== Pipeline Execution Complete ===")
        
    except Exception as e:
        print(f"ERROR: Pipeline execution failed: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

