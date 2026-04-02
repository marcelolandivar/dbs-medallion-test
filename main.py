# =========================
# main.py
# =========================
import os
from pipelines.bronze.load_to_bronze import run_bronze
from pipelines.silver.load_to_silver import run_silver
from pipelines.gold.load_to_gold import run_gold


if __name__ == "__main__":
    env_var = os.getenv("ENVIRONMENT")

    # Execute layers sequentially
    run_bronze(env_var)
    run_silver(env_var)
    run_gold(env_var)