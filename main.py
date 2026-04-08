# =========================
# main.py
# =========================
from pipelines.bronze.load_to_bronze import run_bronze
from pipelines.silver.load_to_silver import run_silver
from pipelines.gold.load_to_gold import run_gold


if __name__ == "__main__":
    # Get the environment from the query parameter
    env_var = dbutils.widgets.get("env")
    print(f"Running with environment: {env_var}")

    # Execute layers sequentially
    run_bronze(env_var)
    run_silver(env_var)
    run_gold(env_var)
