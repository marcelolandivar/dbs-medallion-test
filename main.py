# =========================
# main.py
# =========================
from pyspark.sql import SparkSession
from pipelines.bronze.load_to_bronze import run_bronze
from pipelines.silver.load_to_silver import run_silver
from pipelines.gold.load_to_gold import run_gold
import argparse



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

    # Execute layers sequentially
    run_bronze(env_var)
    run_silver(env_var)
    run_gold(env_var)

