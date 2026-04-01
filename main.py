# =========================
# main.py
# =========================
import sys

from pipelines.bronze import run_bronze
from pipelines.silver import run_silver
from pipelines.gold import run_gold


if __name__ == "__main__":
    env = sys.argv[1]  # dev or prod

    # Execute layers sequentially
    run_bronze(env)
    run_silver(env)
    run_gold(env)