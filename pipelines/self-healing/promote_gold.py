# Databricks notebook source
# MAGIC %md
# MAGIC ## Task 5c — Promote to gold
# MAGIC Writes the daily revenue aggregate to the gold table.
# MAGIC Only runs when the quality gate fully passed.

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
catalog = dbutils.widgets.get("catalog")

severity = dbutils.jobs.taskValues.get(
    taskKey="ai_agent_analyse", key="severity", debugValue="passed")

if severity != "passed":
    dbutils.notebook.exit(
        f"skipped — quality gate not passed (severity='{severity}')")

# COMMAND ----------
spark.sql(f"""
    INSERT OVERWRITE {catalog}.gold.daily_orders
    SELECT
        date_trunc('day', created_at) AS day,
        COUNT(*)                       AS order_count,
        SUM(amount)                    AS revenue,
        AVG(amount)                    AS avg_order_value,
        current_timestamp()            AS refreshed_at
    FROM {catalog}.silver.orders
    GROUP BY 1
""")

count = spark.sql(
    f"SELECT COUNT(*) AS n FROM {catalog}.gold.daily_orders"
).collect()[0]["n"]
print(f"Gold table refreshed — {count} daily partitions written.")