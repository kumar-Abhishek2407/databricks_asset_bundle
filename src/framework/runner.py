"""Run business modules selected by job configuration."""
from __future__ import annotations

from pathlib import Path, PurePosixPath, PureWindowsPath
import runpy
from dataclasses import replace
from typing import Any

from .context import PipelineContext, job_parameters_from_widgets, load_context_from_job


class PipelineRunner:
    def __init__(self, dbutils: Any):
        self.dbutils = dbutils

    def script_path(self, ctx: PipelineContext) -> Path:
        relative = ctx.module
        relative_path = PurePosixPath(relative)
        if (
            not relative
            or "\\" in relative
            or ":" in relative
            or relative_path.is_absolute()
            or PureWindowsPath(relative).drive
            or ".." in relative_path.parts
            or not relative_path.parts
            or relative_path.parts[0] != "pipeline_modules"
            or relative_path.suffix != ".py"
        ):
            raise ValueError(f"Module must be a relative .py path inside pipeline_modules: {relative}")
        root = Path(ctx.workspace_root).resolve()
        modules_root = root / "pipeline_modules"
        if not modules_root.resolve().is_relative_to(root):
            raise ValueError("pipeline_modules must stay inside workspace_root")
        path = (root / relative).resolve()
        if not path.is_relative_to(modules_root.resolve()):
            raise ValueError(f"Module is outside pipeline_modules: {relative}")
        if not path.is_file():
            raise FileNotFoundError(f"Business script not found: {path}")
        return path

    def run(self, ctx: PipelineContext) -> str:
        path = self.script_path(ctx)
        from .spark_helpers import get_spark

        print(f"Starting {ctx.module} in {ctx.env} -> {path}")
        # Exceptions propagate to the job task; Jobs manages stage dependencies.
        namespace = runpy.run_path(str(path))
        entrypoint = namespace.get("run")
        if not callable(entrypoint):
            raise ValueError(f"Business script must define run(ctx): {path}")
        entrypoint(replace(ctx, spark=get_spark()))
        print(f"Finished {ctx.module}")
        return "SUCCESS"


def run_from_widgets(dbutils: Any) -> str:
    ctx = load_context_from_job(job_parameters_from_widgets(dbutils))
    return PipelineRunner(dbutils).run(ctx)
