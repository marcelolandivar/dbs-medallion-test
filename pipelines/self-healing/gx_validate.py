# notebooks/04_ai_agent.py
import json
from openai import AzureOpenAI

run_id = dbutils.jobs.taskValues.get(taskKey="gx_validate", key="run_id")

result_row = spark.sql(f"""
    SELECT result_json FROM catalog.monitoring.dq_results
    WHERE run_id = '{run_id}'
    ORDER BY ts DESC LIMIT 1
""").collect()[0]

result_json = json.loads(result_row["result_json"])
failed_expectations = [r for r in result_json["results"] if not r["success"]]

if not failed_expectations:
    dbutils.jobs.taskValues.set(key="severity", value="passed")
    dbutils.notebook.exit("passed")

client = AzureOpenAI(
    azure_endpoint=dbutils.secrets.get("kv-scope", "azure-openai-endpoint"),
    api_key=dbutils.secrets.get("kv-scope", "azure-openai-key"),
    api_version="2024-02-01"
)

response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": f"""
        Analyse these Great Expectations failures and return JSON with:
        - severity: "critical" | "warning"
        - root_cause: one of [schema_change, source_outage, upstream_bug, data_drift, volume_anomaly]
        - summary: one sentence for a Slack alert
        - remediation: concrete next step for the data engineer
        - affected_downstream: list of table or dashboard names at risk

        Failures:
        {json.dumps(failed_expectations, indent=2)}
    """}],
    response_format={"type": "json_object"}
)

analysis = json.loads(response.choices[0].message.content)

# Write analysis to monitoring table
spark.createDataFrame([{
    "run_id":    run_id,
    "severity":  analysis["severity"],
    "root_cause": analysis["root_cause"],
    "summary":   analysis["summary"],
    "remediation": analysis["remediation"],
    "affected_downstream": json.dumps(analysis.get("affected_downstream", []))
}]).write.format("delta").mode("append").saveAsTable("catalog.monitoring.dq_analysis")

# Signal to branch tasks
dbutils.jobs.taskValues.set(key="severity",   value=analysis["severity"])
dbutils.jobs.taskValues.set(key="root_cause", value=analysis["root_cause"])