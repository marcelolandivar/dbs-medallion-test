# Databricks notebook source
# MAGIC %md
# MAGIC ## Task 5a — Critical action
# MAGIC Quarantines bad rows, deletes them from silver, and fires a Teams alert.
# MAGIC Exits early if severity is not "critical".

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
catalog = dbutils.widgets.get("catalog")

severity = dbutils.jobs.taskValues.get(taskKey="ai_agent_analyse", key="severity",   debugValue="critical")
table    = dbutils.jobs.taskValues.get(taskKey="gx_validate",      key="dq_table",   debugValue="main.silver.orders")
summary  = dbutils.jobs.taskValues.get(taskKey="ai_agent_analyse", key="summary",    debugValue="Test summary")
cause    = dbutils.jobs.taskValues.get(taskKey="ai_agent_analyse", key="root_cause", debugValue="unknown")

if severity != "critical":
    dbutils.notebook.exit(f"skipped — severity is '{severity}', not critical")

# COMMAND ----------
# Ensure quarantine table exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.monitoring.quarantine
    USING DELTA
    AS SELECT *, current_timestamp() AS quarantined_at, '' AS quarantine_reason
    FROM {table} WHERE 1=0
""")

# Move bad rows to quarantine
spark.sql(f"""
    INSERT INTO {catalog}.monitoring.quarantine
    SELECT *, current_timestamp(), '{cause}'
    FROM {table}
    WHERE order_id IS NULL OR amount < 0 OR amount > 100000
""")

# Hard-delete from silver
spark.sql(f"""
    DELETE FROM {table}
    WHERE order_id IS NULL OR amount < 0 OR amount > 100000
""")

quarantined = spark.sql(
    f"SELECT COUNT(*) AS n FROM {catalog}.monitoring.quarantine"
).collect()[0]["n"]
print(f"Quarantined {quarantined} rows from {table}")

# COMMAND ----------
# Send Teams alert
import requests

webhook_url = dbutils.secrets.get("kv-scope", "teams-webhook-url")
payload = {
    "@type": "MessageCard",
    "themeColor": "FF0000",
    "summary": "Critical DQ failure",
    "sections": [{
        "activityTitle": f"[CRITICAL] Data quality failure on `{table}`",
        "facts": [
            {"name": "Root cause",   "value": cause},
            {"name": "Summary",      "value": summary},
            {"name": "Rows removed", "value": str(quarantined)},
        ]
    }]
}
resp = requests.post(webhook_url, json=payload, timeout=10)
print(f"Teams alert HTTP status: {resp.status_code}")