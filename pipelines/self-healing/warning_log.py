# Databricks notebook source


dbutils.widgets.text("catalog", "main")
catalog = dbutils.widgets.get("catalog")

severity = dbutils.jobs.taskValues.get(taskKey="ai_agent_analyse", key="severity",  debugValue="warning")
table    = dbutils.jobs.taskValues.get(taskKey="gx_validate",      key="dq_table",  debugValue="main.silver.orders")
summary  = dbutils.jobs.taskValues.get(taskKey="ai_agent_analyse", key="summary",   debugValue="Test warning")

if severity != "warning":
    dbutils.notebook.exit(f"skipped — severity is '{severity}', not warning")

from datetime import datetime

# Tag the table in Unity Catalog
spark.sql(f"""
    ALTER TABLE {table}
    SET TBLPROPERTIES (
        'dq.last_warning'    = '{summary[:200]}',
        'dq.last_warning_ts' = '{datetime.utcnow().isoformat()}'
    )
""")

# Rebuild Data Docs so the GX portal reflects the latest run
import great_expectations as gx
context = gx.get_context(context_root_dir="/dbfs/gx")
context.build_data_docs()

print(f"Warning logged on table: {table}")
print(f"Summary: {summary}")