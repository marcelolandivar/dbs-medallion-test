# Databricks notebook source
# MAGIC %md
# MAGIC ## Task 4 — AI agent analysis
# MAGIC Reads the GX ValidationResult from the monitoring table,
# MAGIC sends failed expectations to Azure OpenAI, and writes a structured
# MAGIC finding (severity, root cause, remediation) back to monitoring.

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
catalog = dbutils.widgets.get("catalog")

run_id = dbutils.jobs.taskValues.get(taskKey="gx_validate", key="run_id",     debugValue="debug-run-1")
table  = dbutils.jobs.taskValues.get(taskKey="gx_validate", key="dq_table",   debugValue="main.silver.orders")
passed = dbutils.jobs.taskValues.get(taskKey="gx_validate", key="dq_success", debugValue="True")

# COMMAND ----------
# Short-circuit if everything passed — no LLM call needed
if passed == "True":
    dbutils.jobs.taskValues.set(key="severity",   value="passed")
    dbutils.jobs.taskValues.set(key="root_cause", value="none")
    dbutils.jobs.taskValues.set(key="summary",    value="All expectations passed.")
    dbutils.notebook.exit("passed")

# COMMAND ----------
import json
from openai import AzureOpenAI
from datetime import datetime

result_row = spark.sql(f"""
    SELECT result_json FROM {catalog}.monitoring.dq_results
    WHERE run_id = '{run_id}'
    ORDER BY validated_at DESC LIMIT 1
""").collect()[0]

result_json         = json.loads(result_row["result_json"])
failed_expectations = [r for r in result_json.get("results", []) if not r.get("success")]

# COMMAND ----------
client = AzureOpenAI(
    azure_endpoint=dbutils.secrets.get("kv-scope", "azure-openai-endpoint"),
    api_key=dbutils.secrets.get("kv-scope",        "azure-openai-key"),
    api_version="2025-04-14"
)

SYSTEM_PROMPT = (
    "You are a senior data quality engineer analysing Great Expectations "
    "validation failures in an Azure Databricks Lakehouse. Be concise and precise."
)

USER_PROMPT = f"""Analyse these Great Expectations failures and return a single JSON object with:
- severity: "critical" or "warning"
  (critical = blocks downstream gold table or SLA breach; warning = pipeline can continue)
- root_cause: one of [schema_change, source_outage, upstream_bug, data_drift, volume_anomaly, unknown]
- summary: one sentence for a Teams/Slack alert (max 120 chars)
- remediation: concrete next step for the on-call data engineer (max 200 chars)
- affected_downstream: list of table or dashboard names likely impacted

Failures:
{json.dumps(failed_expectations, indent=2)}
"""

response = client.chat.completions.create(
    model="gpt-4.1-mini",
    messages=[
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": USER_PROMPT}
    ],
    response_format={"type": "json_object"},
    temperature=0
)

analysis = json.loads(response.choices[0].message.content)

# COMMAND ----------
# Persist analysis to monitoring table
spark.createDataFrame([{
    "run_id":              run_id,
    "table_name":          table,
    "severity":            analysis["severity"],
    "root_cause":          analysis["root_cause"],
    "summary":             analysis["summary"],
    "remediation":         analysis["remediation"],
    "affected_downstream": json.dumps(analysis.get("affected_downstream", [])),
    "analysed_at":         datetime.utcnow().isoformat()
}]).write.format("delta").mode("append") \
  .saveAsTable(f"{catalog}.monitoring.dq_analysis")

# Propagate to branch tasks
dbutils.jobs.taskValues.set(key="severity",   value=analysis["severity"])
dbutils.jobs.taskValues.set(key="root_cause", value=analysis["root_cause"])
dbutils.jobs.taskValues.set(key="summary",    value=analysis["summary"])

print(f"Severity    : {analysis['severity']}")
print(f"Root cause  : {analysis['root_cause']}")
print(f"Summary     : {analysis['summary']}")
print(f"Remediation : {analysis['remediation']}")
