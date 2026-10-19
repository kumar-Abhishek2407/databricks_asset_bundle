# DAB Project

Databricks Jobs runs four Citibike ETL tasks: Bronze -> Silver -> two parallel Gold tasks.

## Execution

```text
databricks.yml                         deployment targets and variables
  -> resources/citibike.job.yml   schedule, dependencies, task inputs
     -> src/framework/run_pipeline.py  shared job entrypoint
        -> context.py                    loads runtime_config/config.json[environment]
        -> runner.py                     calls the configured module's run(ctx)
              -> pipeline_modules/citibike_etl/{bronze,silver,gold}/*.py
```

The framework owns job widgets, environment loading, Spark session setup, script
selection, execution, and completion logging. Each business module defines `run(ctx)`. The framework loads the module and calls
that function with the environment configuration and Spark session in `ctx`. Script errors
propagate to the Databricks task; Jobs manages dependencies and downstream execution.
There are no nested notebook runs or success-exit calls in business scripts.

Pipeline modules contain transformations and data reads/writes, using environment
settings from `ctx.runtime_config`. The scripts use `ctx.spark` and have no framework imports.

## Configuration

`runtime_config/config.json` holds each environment's Citibike catalog, schemas,
source volume and filename. The job passes `environment`, `workspace_root`, and `module` to the framework.
Optional bundle catalog/schema widgets remain available.

`ctx.runtime_config.get("catalog", "")` selects the operational catalog (`citibike_dev`
or `citibike_test`). Bundle catalog/schema settings are available in `ctx.bundle`.
No run metadata is collected or written to output tables.

## Deployment and invocation

Authenticate to the workspace configured for the chosen target, then run:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev citibike
```

Use `-t test` for the test workspace. The job can also be started with Run now in
Databricks. Its schedule explicitly requests daily 06:00 Asia/Kolkata, UNPAUSED.
Source catalogs and volumes must exist, and the execution identity needs read/write
and schema creation permissions. The configured CSV is processed with overwrite
writes on each execution.

## Adding a pipeline

Create a business module defining `run(ctx)` and add a job YAML under `resources/`
reusing `src/framework/run_pipeline.py`. Set `module` to the file's path relative to
the bundle root, for example `pipeline_modules/delhi_metro/bronze/ingest.py`.
No framework or registry changes are needed. The runner accepts only `.py` files
inside `pipeline_modules` and rejects absolute paths, traversal, and resolved paths
outside that directory.
Add environment-specific settings to `runtime_config/config.json` as needed.

## Local verification

```bash
python -m pytest tests/test_framework.py -q
```

Local tests verify configuration, script execution, failure propagation, and resource
paths. Actual Spark processing requires validation and execution in Databricks.

## Business module contract

```python
def run(ctx):
    spark = ctx.spark
    catalog = ctx.runtime_config.get("catalog", "")
    # Read, transform, and write business data here.
```

The framework loads only config.json[environment] into ctx.runtime_config.
Current targets are dev and test. A future qa target selects the qa section in the
same way when its job passes environment=qa. Configuration keys are quoted Python
strings, for example `.get("catalog", "")`.
