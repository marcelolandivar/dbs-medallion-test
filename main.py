# =========================
# main.py
# =========================
import sys

from pipelines.bronze import bronze_orders
from pipelines.silver import silver_orders
from pipelines.gold import gold_sales


if __name__ == "__main__":
    env = sys.argv[1]  # dev or prod

    # Execute layers sequentially
    bronze_orders.run(env)
    silver_orders.run(env)
    gold_sales.run(env)