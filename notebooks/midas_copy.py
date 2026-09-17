# Databricks notebook source
# MAGIC %md
# MAGIC # Midas
# MAGIC
# MAGIC **Midas** is a small table-copy framework for Databricks.
# MAGIC
# MAGIC Phase 1 does only a few things:
# MAGIC 1. Read a source table.
# MAGIC 2. Optionally select only specific columns.
# MAGIC 3. Apply a very small transformation.
# MAGIC 4. Write the result to a destination table.
# MAGIC
# MAGIC The goal is to keep this notebook understandable first, then make it more sophisticated later.

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

# Restart Python after installing libraries if Databricks asks you to.
# dbutils.library.restartPython()

# COMMAND ----------

import yaml

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Notebook parameters
# MAGIC
# MAGIC These widgets let the same notebook run for different tables/configs.
# MAGIC
# MAGIC For a Databricks Workflow / Job, these can be passed in as task parameters.

# COMMAND ----------

dbutils.widgets.text("table_name", "")
dbutils.widgets.text("tables_config_path", "/Workspace/Repos/<you>/midas-db/config/tables.yml")
dbutils.widgets.text("transform_config_path", "/Workspace/Repos/<you>/midas-db/config/transformations.yml")

table_name = dbutils.widgets.get("table_name").strip()
tables_config_path = dbutils.widgets.get("tables_config_path")
transform_config_path = dbutils.widgets.get("transform_config_path")

print(f"Requested table: {table_name or '<not supplied>'}")
print(f"Tables config: {tables_config_path}")
print(f"Transform config: {transform_config_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helper: load YAML
# MAGIC
# MAGIC For this starter version, we read YAML directly from the Databricks workspace filesystem.
# MAGIC
# MAGIC Later, you could move configuration into:
# MAGIC - Unity Catalog volumes
# MAGIC - DBFS
# MAGIC - a Git repo checked out as a Databricks Repo
# MAGIC - a Delta configuration table

# COMMAND ----------

def load_yaml(path: str) -> dict:
    """
    Load a YAML file and return a Python dictionary.

    Note:
    In Databricks Repos / Workspace files, normal Python file access is usually enough.
    If you move these files to DBFS or Volumes, you may adapt this helper.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


tables_config = load_yaml(tables_config_path)
transform_config = load_yaml(transform_config_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Choose which table definition to run
# MAGIC
# MAGIC `tables.yml` contains a list of copy definitions.
# MAGIC
# MAGIC If `table_name` is provided, we run only that entry.
# MAGIC Otherwise, this demo uses the first enabled table.

# COMMAND ----------

table_entries = tables_config.get("tables", [])

if not table_entries:
    raise ValueError("No tables were found in tables.yml")

enabled_entries = [t for t in table_entries if t.get("enabled", True)]

if table_name:
    matches = [t for t in enabled_entries if t.get("name") == table_name]
    if not matches:
        raise ValueError(f"No enabled table config found with name='{table_name}'")
    table_cfg = matches[0]
else:
    if not enabled_entries:
        raise ValueError("No enabled table definitions were found.")
    table_cfg = enabled_entries[0]

table_cfg

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read the source table

# COMMAND ----------

source_table = table_cfg["source"]
destination_table = table_cfg["destination"]

df = spark.read.table(source_table)

print(f"Read source table: {source_table}")
print(f"Rows: {df.count():,}")
print(f"Columns: {len(df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Optional column selection
# MAGIC
# MAGIC If the YAML includes:
# MAGIC
# MAGIC ```yaml
# MAGIC columns:
# MAGIC   - customer_id
# MAGIC   - order_date
# MAGIC ```
# MAGIC
# MAGIC then Midas only brings those columns.
# MAGIC
# MAGIC If `columns` is omitted or empty, all columns are preserved.

# COMMAND ----------

requested_columns = table_cfg.get("columns") or []

if requested_columns:
    missing_columns = [c for c in requested_columns if c not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Configured columns are missing from {source_table}: {missing_columns}"
        )

    df = df.select(*requested_columns)

print("Selected columns:", df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Apply a phase-one transformation
# MAGIC
# MAGIC For phase one, Midas supports only:
# MAGIC
# MAGIC - `none`
# MAGIC - `add_column`
# MAGIC
# MAGIC This is deliberately small.
# MAGIC
# MAGIC Later, you can expand this into a transformation registry such as:
# MAGIC
# MAGIC ```python
# MAGIC TRANSFORMS = {
# MAGIC     "add_column": add_column,
# MAGIC     "rename_column": rename_column,
# MAGIC     "filter_rows": filter_rows,
# MAGIC }
# MAGIC ```

# COMMAND ----------

def apply_transformation(df: DataFrame, transform_name: str, config: dict) -> DataFrame:
    """
    Apply one named transformation.

    Phase 1:
      - none
      - add_column
    """

    if not transform_name or transform_name == "none":
        return df

    if transform_name == "add_column":
        column_name = config["column_name"]
        value = config.get("value")
        return df.withColumn(column_name, F.lit(value))

    raise ValueError(f"Unsupported transformation: {transform_name}")

# COMMAND ----------

transform_name = table_cfg.get("transformation", "none")

all_transformations = transform_config.get("transformations", {})
selected_transform_config = all_transformations.get(transform_name, {})

df = apply_transformation(
    df=df,
    transform_name=transform_name,
    config=selected_transform_config,
)

print(f"Applied transformation: {transform_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Preview before writing

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write the destination table
# MAGIC
# MAGIC This version uses Delta and `overwrite` by default.

# COMMAND ----------

write_mode = table_cfg.get("write_mode", "overwrite")

(
    df.write
      .format("delta")
      .mode(write_mode)
      .option("overwriteSchema", "true")
      .saveAsTable(destination_table)
)

print(f"Wrote destination table: {destination_table}")
print(f"Write mode: {write_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Basic validation

# COMMAND ----------

source_count = spark.read.table(source_table).count()
destination_count = spark.read.table(destination_table).count()

print(f"Source rows:      {source_count:,}")
print(f"Destination rows: {destination_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Where Midas goes next
# MAGIC
# MAGIC **Phase 1**
# MAGIC - copy table
# MAGIC - choose columns
# MAGIC - add literal column
# MAGIC
# MAGIC **Phase 2**
# MAGIC - multiple transforms per table
# MAGIC - transformation registry
# MAGIC - validation
# MAGIC - audit metadata
# MAGIC
# MAGIC **Phase 3**
# MAGIC - incremental loads
# MAGIC - merge/upsert semantics
# MAGIC - dependencies / ordering
# MAGIC - Databricks Workflows orchestration
# MAGIC - configuration validation
