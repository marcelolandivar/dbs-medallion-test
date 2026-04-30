dbutils.widgets.text("table_name", f"`{cfg.catalog}`.`{cfg.schema}`.`silver_roads`")
dbutils.widgets.text("suite_name", "orders_suite")

table_name = dbutils.widgets.get("table_name")
suite_name  = dbutils.widgets.get("suite_name")

# COMMAND ----------
import great_expectations as gx
import json
from datetime import datetime

context = gx.get_context(context_root_dir="s3://amazn-s3-db-bckt/gx_expectations/")

# Build or reuse the expectation suite
try:
    suite = context.get_expectation_suite(suite_name)
    print(f"Loaded existing suite: {suite_name}")
except Exception:
    print(f"Creating new suite: {suite_name}")
    suite = context.add_expectation_suite(suite_name)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="amount"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="amount", min_value=0, max_value=100_000))
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=1_000, max_value=10_000_000))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="created_at", type_="TimestampType"))
    context.save_expectation_suite(suite)

# COMMAND ----------
# Run the checkpoint
df = spark.read.table(table_name)
batch_request = (context
    .get_datasource("spark_datasource")
    .get_asset("orders")
    .build_batch_request(dataframe=df))

result = context.run_checkpoint(
    checkpoint_name="orders_checkpoint",
    validations=[{
        "batch_request": batch_request,
        "expectation_suite_name": suite_name
    }]
)

success     = result.success
result_json = result.to_json_dict()
run_id      = dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()

# COMMAND ----------
# Persist result to monitoring table
spark.createDataFrame([{
    "run_id":       run_id,
    "table_name":   table_name,
    "suite_name":   suite_name,
    "success":      success,
    "result_json":  json.dumps(result_json),
    "validated_at": datetime.utcnow().isoformat()
}]).write.format("delta").mode("append") \
  .saveAsTable("main.monitoring.dq_results")

# Rebuild Data Docs
context.build_data_docs()

# Signal downstream tasks via task values
dbutils.jobs.taskValues.set(key="run_id",     value=run_id)
dbutils.jobs.taskValues.set(key="dq_success", value=str(success))
dbutils.jobs.taskValues.set(key="dq_table",   value=table_name)

print(f"Validation complete. Success={success}")
if not success:
    raise Exception("GX validation failed — ai_agent_analyse will handle via run_if=ALL_DONE")