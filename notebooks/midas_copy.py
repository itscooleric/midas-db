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

# MAGIC %pip install pyyaml "pydantic>=2,<3"

# COMMAND ----------

# Restart Python after installing libraries if Databricks asks you to.
# dbutils.library.restartPython()

# COMMAND ----------

from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError
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
# MAGIC YAML and Pydantic solve two different problems:
# MAGIC
# MAGIC - **YAML** turns a human-readable file into ordinary Python dictionaries and lists.
# MAGIC - **Pydantic** checks whether that Python data is valid *Midas configuration*.
# MAGIC
# MAGIC Keeping those jobs separate is useful. PyYAML only needs to understand YAML syntax.
# MAGIC Pydantic owns the Midas contract: required fields, allowed values, defaults, and types.
# MAGIC
# MAGIC For this starter version, we read YAML directly from the Databricks workspace filesystem.

# COMMAND ----------

def load_yaml(path: str) -> dict:
    """
    Load a YAML file and return the raw Python dictionary.

    This function intentionally does not decide whether the contents are valid
    Midas configuration. The Pydantic models below handle that separately.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pydantic configuration models
# MAGIC
# MAGIC The YAML file is flexible text. These models are the contract that turns it into
# MAGIC trusted Midas objects.
# MAGIC
# MAGIC A few details are worth noticing:
# MAGIC
# MAGIC - `extra="forbid"` rejects unknown keys. A typo like `destnation` fails immediately.
# MAGIC - `Literal[...]` limits a field to operations Midas actually supports in this phase.
# MAGIC - defaults such as `enabled = True` live in one obvious place.
# MAGIC - `Field(default_factory=list)` gives every table its own empty list when `columns`
# MAGIC   is omitted.
# MAGIC
# MAGIC This is intentionally stricter than reading a dictionary and hoping later code
# MAGIC notices mistakes.

# COMMAND ----------

class StrictConfigModel(BaseModel):
    """
    Base class for Midas configuration models.

    Rejecting extra fields is useful for configuration because misspelled keys should
    be errors, not silently ignored.
    """

    model_config = ConfigDict(extra="forbid")


class TableConfig(StrictConfigModel):
    """One source-to-destination table copy definition."""

    name: str
    enabled: bool = True
    source: str
    destination: str
    columns: list[str] = Field(default_factory=list)

    # Phase 1 supports exactly these transformations.
    transformation: Literal["none", "add_column"] = "none"

    # Phase 1 is deliberately overwrite-only.
    # Append or merge should become explicit future features.
    write_mode: Literal["overwrite"] = "overwrite"


class TablesConfig(StrictConfigModel):
    """Top-level shape of config/tables.yml."""

    tables: list[TableConfig]


class NoTransformationConfig(StrictConfigModel):
    """Configuration for the explicit no-op transformation."""

    pass


class AddColumnConfig(StrictConfigModel):
    """Settings required by the add_column transformation."""

    column_name: str

    # Spark literals can reasonably be strings, numbers, booleans, or null.
    value: str | int | float | bool | None = None


class TransformationDefinitions(StrictConfigModel):
    """Named transformation definitions available to Midas Phase 1."""

    none: NoTransformationConfig = Field(default_factory=NoTransformationConfig)
    add_column: AddColumnConfig


class TransformationsConfig(StrictConfigModel):
    """Top-level shape of config/transformations.yml."""

    transformations: TransformationDefinitions

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Parse first, validate second
# MAGIC
# MAGIC `load_yaml(...)` gives us untrusted dictionaries.
# MAGIC
# MAGIC `model_validate(...)` is the boundary where Pydantic checks those dictionaries
# MAGIC against the Midas models. After this cell succeeds, the rest of the notebook can
# MAGIC work with typed objects such as `table_cfg.source` instead of repeatedly asking
# MAGIC whether dictionary keys exist.
# MAGIC
# MAGIC If a config file is malformed, Midas stops here before Spark reads or writes data.

# COMMAND ----------

def load_tables_config(path: str) -> TablesConfig:
    """Load and validate config/tables.yml."""
    raw_config = load_yaml(path)

    try:
        return TablesConfig.model_validate(raw_config)
    except ValidationError as exc:
        raise ValueError(f"Invalid table configuration in {path}:\n{exc}") from exc


def load_transformations_config(path: str) -> TransformationsConfig:
    """Load and validate config/transformations.yml."""
    raw_config = load_yaml(path)

    try:
        return TransformationsConfig.model_validate(raw_config)
    except ValidationError as exc:
        raise ValueError(f"Invalid transformation configuration in {path}:\n{exc}") from exc


tables_config = load_tables_config(tables_config_path)
transform_config = load_transformations_config(transform_config_path)

print(f"Validated {len(tables_config.tables)} table definition(s).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Choose which table definition to run
# MAGIC
# MAGIC `tables.yml` contains a list of copy definitions.
# MAGIC
# MAGIC If `table_name` is provided, we run only that entry.
# MAGIC Otherwise, this demo uses the first enabled table.
# MAGIC
# MAGIC Notice that `table_cfg` is now a `TableConfig` object, not a free-form dictionary.

# COMMAND ----------

table_entries = tables_config.tables

if not table_entries:
    raise ValueError("No tables were found in tables.yml")

enabled_entries = [table for table in table_entries if table.enabled]

if table_name:
    matches = [table for table in enabled_entries if table.name == table_name]

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
# MAGIC ## 6. Read the source table

# COMMAND ----------

source_table = table_cfg.source
destination_table = table_cfg.destination

df = spark.read.table(source_table)

print(f"Read source table: {source_table}")
print(f"Rows: {df.count():,}")
print(f"Columns: {len(df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Optional column selection
# MAGIC
# MAGIC A YAML `columns` list tells Midas which source columns to keep.
# MAGIC
# MAGIC If `columns` is omitted or empty, Pydantic supplies an empty list and all columns
# MAGIC are preserved.

# COMMAND ----------

requested_columns = table_cfg.columns

if requested_columns:
    missing_columns = [column for column in requested_columns if column not in df.columns]

    if missing_columns:
        raise ValueError(
            f"Configured columns are missing from {source_table}: {missing_columns}"
        )

    df = df.select(*requested_columns)

print("Selected columns:", df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Apply a phase-one transformation
# MAGIC
# MAGIC For phase one, Midas supports only `none` and `add_column`.
# MAGIC
# MAGIC Pydantic has already guaranteed that the table cannot name an unsupported
# MAGIC transformation and that `add_column` has the settings it requires.
# MAGIC
# MAGIC Later, this can expand into the transformation registry described in the README.

# COMMAND ----------

def apply_transformation(
    df: DataFrame,
    transform_name: Literal["none", "add_column"],
    config: NoTransformationConfig | AddColumnConfig,
) -> DataFrame:
    """
    Apply one validated transformation.

    The config object has already passed Pydantic validation before this function runs.
    This function therefore focuses on execution rather than defensive dictionary checks.
    """

    if transform_name == "none":
        return df

    if transform_name == "add_column":
        # Pydantic validated the required column_name and optional value.
        return df.withColumn(config.column_name, F.lit(config.value))

    # The Literal type and Pydantic validation make this unreachable in normal use.
    raise ValueError(f"Unsupported transformation: {transform_name}")

# COMMAND ----------

transform_name = table_cfg.transformation

selected_transform_config = getattr(
    transform_config.transformations,
    transform_name,
)

df = apply_transformation(
    df=df,
    transform_name=transform_name,
    config=selected_transform_config,
)

print(f"Applied transformation: {transform_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Preview before writing

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Write the destination table
# MAGIC
# MAGIC This version uses Delta and deliberately supports only `overwrite`.
# MAGIC
# MAGIC Because `write_mode` is a Pydantic `Literal["overwrite"]`, adding append or merge
# MAGIC later requires an explicit change to the Midas contract instead of silently
# MAGIC accepting a new mode.

# COMMAND ----------

write_mode = table_cfg.write_mode

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
# MAGIC ## 11. Basic validation

# COMMAND ----------

source_count = spark.read.table(source_table).count()
destination_count = spark.read.table(destination_table).count()

print(f"Source rows:      {source_count:,}")
print(f"Destination rows: {destination_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Where Midas goes next
# MAGIC
# MAGIC **Phase 1**
# MAGIC - copy table
# MAGIC - choose columns
# MAGIC - validate configuration with Pydantic
# MAGIC - add literal column
# MAGIC
# MAGIC **Phase 2**
# MAGIC - multiple transforms per table
# MAGIC - transformation registry
# MAGIC - richer validation rules
# MAGIC - audit metadata
# MAGIC
# MAGIC **Phase 3**
# MAGIC - incremental loads
# MAGIC - merge/upsert semantics
# MAGIC - dependencies / ordering
# MAGIC - Databricks Workflows orchestration
