"""Single source of truth for environment-specific configuration.

Bundle target -> job widgets -> runtime context -> business script.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, Mapping


_REQUIRED_WIDGETS = ("environment", "workspace_root", "pipeline", "stage")
_OPTIONAL_WIDGETS = ("bundle_catalog", "bundle_schema")


@dataclass(frozen=True)
class PipelineContext:
    environment: str
    workspace_root: str
    pipeline: str
    stage: str
    config: dict[str, Any] = field(default_factory=dict)
    bundle: dict[str, str] = field(default_factory=dict)
    spark: Any = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not self.environment or not self.environment.strip():
            raise ValueError("environment must not be empty")
        if not Path(self.workspace_root).is_absolute():
            raise ValueError("workspace_root must be an absolute workspace file path")
        if not isinstance(self.config, dict):
            raise TypeError("config must be a JSON object")

    @property
    def runtime_config(self) -> dict[str, Any]:
        """Settings for the environment selected by the job."""
        return self.config

    @property
    def env(self) -> str:
        """Convenience alias used by business logic."""
        return self.environment

    def require(self, key: str) -> Any:
        """Return a runtime setting, failing immediately if missing."""
        if key not in self.config:
            raise KeyError(f"Missing '{key}' in config for environment '{self.environment}'")
        return self.config[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)

    def to_json(self) -> str:
        """Serialize context for inspection or local tooling."""
        return json.dumps(
            {
                "environment": self.environment,
                "workspace_root": self.workspace_root,
                "pipeline": self.pipeline,
                "stage": self.stage,
                "config": self.config,
                "bundle": self.bundle,
            }, ensure_ascii=True
        )

    @classmethod
    def from_json(cls, payload: str) -> PipelineContext:
        obj = json.loads(payload)
        if not isinstance(obj, dict):
            raise ValueError("context_json must be a JSON object")
        return cls(**obj)


def load_context_from_job(parameters: Mapping[str, str], config_path: str | Path | None = None) -> PipelineContext:
    """Read config.json for the environment passed by the deployed job.

    `config_path` is only for tests/local development; in Databricks the absolute
    workspace_root value is provided by the bundle as ${workspace.file_path}.
    """
    missing = [key for key in _REQUIRED_WIDGETS if not parameters.get(key)]
    if missing:
        raise ValueError(f"Missing required job parameter(s): {', '.join(missing)}")

    root = Path(parameters["workspace_root"])
    path = Path(config_path) if config_path is not None else root / "runtime_config" / "config.json"
    try:
        with path.open(encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError as error:
        raise FileNotFoundError(f"Runtime config not found: {path}. Check DAB file sync.") from error

    if not isinstance(data, dict):
        raise ValueError(f"Runtime config at {path} must contain a JSON object")

    env = parameters["environment"].strip()
    if env not in data:
        raise KeyError(f"Environment '{env}' is absent from {path}. Available: {', '.join(data.keys())}")
    if not isinstance(data[env], dict):
        raise ValueError(f"Configuration for '{env}' must be an object")

    return PipelineContext(
        environment=env,
        workspace_root=str(root),
        pipeline=parameters["pipeline"],
        stage=parameters["stage"],
        config=dict(data[env]),
        bundle={
            "catalog": parameters.get("bundle_catalog", ""),
            "schema": parameters.get("bundle_schema", ""),
        },
    )


def job_parameters_from_widgets(dbutils: Any) -> dict[str, str]:
    """Read job inputs; fails for missing mandatory widgets, never silently assumes 'dev'."""
    parameters: dict[str, str] = {}
    for name in _REQUIRED_WIDGETS + _OPTIONAL_WIDGETS:
        try:
            parameters[name] = dbutils.widgets.get(name)
        except Exception as error:
            if name in _REQUIRED_WIDGETS:
                raise ValueError(f"Missing required Databricks widget: {name}") from error
            parameters[name] = ""
    return parameters
