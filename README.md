# Midas

**Midas** is a deliberately small Databricks / PySpark starter project for copying tables from one location to another and applying a small, controlled set of transformations.

The name comes from the basic idea: Midas takes existing tables and turns them into your managed **gold** tables.

## Phase 1

The first version is intentionally simple:

- Read a table with `spark.read.table(...)`
- Optionally select a subset of columns
- Apply either:
  - no transformation, or
  - add one literal column
- Write the result as a Delta table
- Configure the behavior with YAML
- Run the same notebook with parameters

This is meant to be easy to understand and extend, not a complete data engineering framework.

## Repository layout

```text
midas-db/
├── notebooks/
│   └── midas_copy.py
├── config/
│   ├── tables.yml
│   └── transformations.yml
└── README.md
```

## Core idea

A table definition lives in `config/tables.yml`.

Example:

```yaml
tables:
  - name: customers
    source: source_catalog.source_schema.customers
    destination: my_catalog.my_schema.gold_customers

    columns:
      - customer_id
      - customer_name
      - created_at

    transformation: add_column
    write_mode: overwrite
```

The corresponding transformation settings live in `config/transformations.yml`:

```yaml
transformations:
  none: {}

  add_column:
    column_name: midas_source
    value: copied_by_midas
```

## Running the notebook

The notebook defines three Databricks widgets:

- `table_name`
- `tables_config_path`
- `transform_config_path`

You can run the notebook interactively or pass those values from a Databricks Workflow.

For example:

```text
table_name = customers
tables_config_path = /Workspace/Repos/<you>/midas-db/config/tables.yml
transform_config_path = /Workspace/Repos/<you>/midas-db/config/transformations.yml
```

If `table_name` is blank, the demo notebook runs the first enabled table definition.

## How the notebook works

Conceptually, Midas phase one is only:

```python
df = spark.read.table(source_table)

if configured_columns:
    df = df.select(*configured_columns)

df = apply_transformation(df, transform_name, transform_config)

(
    df.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable(destination_table)
)
```

Everything else in the starter notebook is there to make that process safer and easier to learn.

## Recommended learning path

Before adding more features, make sure you are comfortable with these pieces individually:

1. `spark.read.table`
2. DataFrame `.select(...)`
3. `withColumn(...)`
4. `functions.lit(...)`
5. Delta `.saveAsTable(...)`
6. Databricks widgets
7. Reading YAML into Python dictionaries

Once those pieces feel obvious, the framework can grow naturally.

## Good Phase 2 additions

The first major architectural improvement should probably be a **transformation registry**.

Instead of a long chain of `if` statements:

```python
TRANSFORMS = {
    "add_column": add_column,
    "rename_column": rename_column,
    "filter_rows": filter_rows,
}
```

Then a table could eventually declare multiple transforms:

```yaml
transformations:
  - type: add_column
    column_name: source_system
    value: crm

  - type: rename_column
    from: cust_id
    to: customer_id
```

Other useful next steps:

- multiple transformations per table
- schema checks
- row-count reconciliation
- audit columns
- logging to a Delta audit table
- incremental loads
- merge/upsert support
- dependency ordering
- YAML schema validation
- Databricks Workflows orchestration

## A useful design rule

Keep the YAML **declarative**.

Good:

```yaml
transformation: add_column
```

Less good:

```yaml
python_code: "df.withColumn(...)"
```

Midas should decide what operations are allowed. The configuration should describe intent rather than execute arbitrary code.

That keeps the system understandable, testable, and maintainable.

## One important production note

The starter notebook defaults to `overwrite` because it is easy to reason about.

Do not assume overwrite is the correct long-term strategy for every table. Different datasets may eventually require:

- full refresh
- append
- merge/upsert
- change-data capture
- partition replacement

Treat write strategy as part of the table contract.

## Philosophy

Midas should start boring.

A small amount of explicit PySpark plus declarative configuration is easier to maintain than a large framework built before the use cases are understood.

Build the copy loop first. Add abstractions only when repeated patterns make them necessary.
