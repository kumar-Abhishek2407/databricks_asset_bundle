"""Run registered business scripts inside the framework job notebook."""
from __future__ import annotations

from pathlib import Path, PurePosixPath
import runpy
from dataclasses import replace
from typing import Any

from .context import PipelineContext, job_parameters_from_widgets, load_context_from_job
from .registry import PIPELINE_SCRIPTS


class PipelineRunner:
    def __init__(self, dbutils: Any):
        self.dbutils = dbutils

    def script_path(self, ctx: PipelineContext) -> Path:
        try:
            relative = PIPELINE_SCRIPTS[ctx.pipeline][ctx.stage]
        except KeyError as error:
            raise ValueError(
                f"Unknown pipeline/stage: {ctx.pipeline}/{ctx.stage}. "
                "Register the script in src/framework/registry.py"
            ) from error
        relative_path = PurePosixPath(relative)
        if relative_path.is_absolute() or '..' in relative_path.parts:
            raise ValueError(f"Unsafe registry script path: {relative}")
        root = Path(ctx.workspace_root).resolve()
        path = (root / relative).resolve()
        if not path.is_relative_to(root):
            raise ValueError(f"Script is outside workspace_root: {relative}")
        if not path.is_file():
            raise FileNotFoundError(f"Business script not found: {path}")
        return path

    def run(self, ctx: PipelineContext) -> str:
        path = self.script_path(ctx)
        from .spark_helpers import get_spark

        print(f"Starting {ctx.pipeline}/{ctx.stage} in {ctx.env} -> {path}")
        # Exceptions propagate to the job task; Jobs manages stage dependencies.
        namespace = runpy.run_path(str(path))
        entrypoint = namespace.get("run")
        if not callable(entrypoint):
            raise ValueError(f"Business script must define run(ctx): {path}")
        entrypoint(replace(ctx, spark=get_spark()))
        print(f"Finished {ctx.pipeline}/{ctx.stage}")
        return "SUCCESS"


def run_from_widgets(dbutils: Any) -> str:
    ctx = load_context_from_job(job_parameters_from_widgets(dbutils))
    return PipelineRunner(dbutils).run(ctx)
