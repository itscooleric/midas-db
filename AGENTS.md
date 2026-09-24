# Agent instructions for Midas

Midas is a deliberately small Databricks/PySpark teaching and starter project. Optimize for clarity, explicit contracts, and changes that are easy to verify in Databricks.

## Scope

Phase 1 is intentionally narrow:

- read a source table with Spark;
- optionally select columns;
- apply `none` or `add_column`;
- validate YAML configuration with Pydantic;
- write a Delta destination table with `overwrite`.

Do not expand the framework merely because an abstraction is possible.

## Databricks execution model

- Treat Databricks as the authoritative runtime for notebook behavior.
- Keep notebook-scoped dependencies explicit with `%pip`.
- After notebook-scoped package installation, restart Python before depending on newly installed versions.
- Prefer relative paths inside the Git folder over user-specific absolute workspace paths.
- Do not assume the repository lives under legacy `/Workspace/Repos`; current Git folders may live elsewhere.
- Keep source and destination examples obviously placeholder/test-oriented.
- Never point defaults at production data.

## Configuration

- YAML is the human-facing declarative format.
- Pydantic defines what valid Midas configuration means.
- Keep `extra="forbid"` unless there is a concrete compatibility reason to relax it.
- New transformations or write modes must be added explicitly to the schema and execution logic together.
- Do not allow arbitrary Python/code execution from YAML.

## Safety

- Phase 1 uses `overwrite`; therefore test destinations must be disposable.
- Never run destructive tests against shared or production tables.
- Do not add credentials, tokens, workspace secrets, or environment-specific identifiers to the repository.
- Treat catalog/schema permission failures as environment issues until evidence shows a Midas code defect.

## Verification before merging behavior changes

For the current Pydantic validation work:

1. run the notebook successfully in Databricks with valid config;
2. verify the destination table and row count;
3. misspell a required key and confirm validation fails before Spark execution;
4. use an unsupported transformation and confirm validation fails before Spark execution;
5. restore valid config and rerun successfully.

Do not claim Databricks runtime verification passed if it was not actually executed there.

## Design discipline

Prefer:

- short, explicit PySpark;
- typed configuration objects;
- small teaching-oriented notebook cells;
- comments that explain boundaries and tradeoffs.

Avoid:

- registries before multiple real transformations exist;
- packaging layers before code is reused;
- orchestration before the copy loop is stable;
- production abstractions in a learning repo.
