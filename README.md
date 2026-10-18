# DAB Project

Databricks Jobs runs four Citibike ETL tasks: Bronze -> Silver -> two parallel Gold tasks.

## Execution

```text
databricks.yml                         deployment targets and variables
  -> resources/citibike.job.yml   schedule, dependencies, task inputs
     -> src/framework/run_pipeline.py  shared job entrypoint
        -> context.py                    loads runtime_config/config.json[environment]
        -> runner.py                     provides spark and ctx; executes a script
           -> registry.py                maps pipeline/stage to a .py file
              -> pipeline_modules/citibike_etl/{bronze,silver,gold}/*.py
```

The framework owns job widgets, environment loading, Spark session setup, script
selection, execution, and completion logging. Each script runs in its own Python
global namespace with `spark` and `ctx` provided by the framework. Script errors
propagate to the Databricks task; Jobs manages dependencies and downstream execution.
There are no nested notebook runs or success-exit calls in business scripts.

Pipeline modules contain transformations and table reads/writes. Shared data helpers
resolve table and volume paths from `ctx` and create destination schemas. The scripts
use the existing Databricks Spark session supplied by the framework.

## Configuration

`runtime_config/config.json` holds each environment's Citibike catalog, schemas,
source volume and filename. The job passes `environment`, `workspace_root`, `pipeline`,
`stage`, `bundle_catalog`, and `bundle_schema` to the framework.

`ctx.require("citibike_catalog")` selects the operational catalog (`citibike_dev`
or `citibike_test`). Bundle catalog/schema settings are available in `ctx.bundle`.
No run metadata is collected or written to output tables.

## Deployment and invocation

Authenticate to the workspace configured for the chosen target, then run:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev citibike_notebook
```

Use `-t test` for the test workspace. The job can also be started with Run now in
Databricks. Its schedule explicitly requests daily 06:00 Asia/Kolkata, UNPAUSED.
Source catalogs and volumes must exist, and the execution identity needs read/write
and schema creation permissions. The configured CSV is processed with overwrite
writes on each execution.

## Adding a pipeline

Create business scripts, register their stage paths in `src/framework/registry.py`,
and add a job YAML under `resources/` reusing `src/framework/run_pipeline.py`.
Add environment-specific settings to `runtime_config/config.json` as needed.

## Local verification

```bash
python -m pytest tests/test_framework.py -q
```

Local tests verify configuration, script execution, failure propagation, and resource
paths. Actual Spark processing requires validation and execution in Databricks.
