# Databricks notebook source
# Thin entrypoint shared by every Databricks Job task.
# Job input values are supplied by each job's base_parameters.
import sys
from pathlib import Path

# Widgets also allow launching this notebook manually with explicit inputs.
for name in (
    "environment", "workspace_root", "bundle_catalog", "bundle_schema",
    "module",
):
    dbutils.widgets.text(name, "")

workspace_root = dbutils.widgets.get("workspace_root")
if not workspace_root or not Path(workspace_root).is_absolute():
    raise ValueError("workspace_root must be passed as ${workspace.file_path} by the bundle")
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)

from src.framework.context import job_parameters_from_widgets, load_context_from_job
from src.framework.runner import PipelineRunner

ctx = load_context_from_job(job_parameters_from_widgets(dbutils))
result = PipelineRunner(dbutils).run(ctx)
print(result)
