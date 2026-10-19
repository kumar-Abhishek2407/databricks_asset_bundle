import json
from pathlib import Path

import pytest

from src.framework.context import PipelineContext, load_context_from_job, job_parameters_from_widgets
from src.framework.runner import PipelineRunner

ROOT = Path(__file__).resolve().parents[1]


def params(env):
    return {
        "environment": env,
        "workspace_root": str(ROOT),
        "pipeline": "citibike_etl",
        "stage": "bronze",
        "bundle_catalog": "dab",
        "bundle_schema": "dev",
    }


@pytest.mark.parametrize("env,catalog", [("dev", "citibike_dev"), ("test", "citibike_test")])
def test_target_specific_configuration(env, catalog):
    ctx = load_context_from_job(params(env))
    assert ctx.env == env
    assert ctx.runtime_config.get("catalog", "") == catalog
    assert ctx.bundle == {"catalog": "dab", "schema": "dev"}


def test_context_round_trip_ascii():
    ctx = load_context_from_job(params("dev"))
    payload = ctx.to_json()
    assert payload.isascii()
    assert PipelineContext.from_json(payload) == ctx


def test_undefined_environment_fails():
    with pytest.raises(KeyError, match="absent"):
        load_context_from_job(params("prod"))


def test_missing_widget_fails():
    missing = params("dev")
    missing.pop("environment")
    with pytest.raises(ValueError, match="environment"):
        load_context_from_job(missing)


class FakeDbutils:
    pass


def test_runner_executes_script_with_context(monkeypatch, tmp_path):
    import sys
    from types import SimpleNamespace
    from src.framework import registry

    script = tmp_path / "business.py"
    output = tmp_path / "result.txt"
    script.write_text("def run(ctx):\n    from pathlib import Path\n    Path(ctx.runtime_config['output']).write_text(str(ctx.spark) + ':' + ctx.env)\n")
    monkeypatch.setitem(registry.PIPELINE_SCRIPTS, "example", {"bronze": "business.py"})
    monkeypatch.setitem(sys.modules, "src.framework.spark_helpers", SimpleNamespace(get_spark=lambda: "fake-spark"))
    ctx = PipelineContext("test", str(tmp_path), "example", "bronze", {"output": str(output)})
    assert PipelineRunner(FakeDbutils()).run(ctx) == "SUCCESS"
    assert output.read_text() == "fake-spark:test"


def test_runner_propagates_business_failure(monkeypatch, tmp_path):
    import sys
    from types import SimpleNamespace
    from src.framework import registry

    (tmp_path / "business.py").write_text("def run(ctx):\n    raise RuntimeError('business failed')")
    monkeypatch.setitem(registry.PIPELINE_SCRIPTS, "example", {"bronze": "business.py"})
    monkeypatch.setitem(sys.modules, "src.framework.spark_helpers", SimpleNamespace(get_spark=lambda: object()))
    ctx = PipelineContext("dev", str(tmp_path), "example", "bronze")
    with pytest.raises(RuntimeError, match="business failed"):
        PipelineRunner(FakeDbutils()).run(ctx)


def test_runner_rejects_unknown_stage():
    ctx = load_context_from_job({**params("dev"), "stage": "invalid"})
    with pytest.raises(ValueError, match="Unknown pipeline/stage"):
        PipelineRunner(FakeDbutils()).run(ctx)


def test_corrupt_config_and_absent_file(tmp_path):
    values = params("dev")
    with pytest.raises(FileNotFoundError, match="Runtime config not found"):
        load_context_from_job(values, tmp_path / "missing.json")
    p = tmp_path / "bad.json"
    p.write_text('{"dev":[]}', encoding="utf-8")
    with pytest.raises(ValueError, match="must be an object"):
        load_context_from_job(values, p)


def test_registered_paths_exist():
    from src.framework.registry import PIPELINE_SCRIPTS
    for stages in PIPELINE_SCRIPTS.values():
        for relative in stages.values():
            assert (ROOT / relative).is_file()


def test_job_notebook_points_to_thin_notebook():
    yaml = pytest.importorskip("yaml")
    root = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    job = yaml.safe_load((ROOT / "resources/citibike.job.yml").read_text(encoding="utf-8"))
    assert {"dev", "test"}.issubset(root["targets"])
    for target_name in ("dev", "test"):
        assert root["targets"][target_name]["variables"]["environment"] == target_name
    tasks = job["resources"]["jobs"]["citibike"]["tasks"]
    assert len(tasks) == 4
    for task in tasks:
        notebook = task["notebook_task"]
        assert (ROOT / "resources" / notebook["notebook_path"]).resolve().is_file()
        assert notebook["base_parameters"]["environment"] == "${var.environment}"
        assert notebook["base_parameters"]["workspace_root"] == "${workspace.file_path}"


def test_widget_reader():
    class Widgets:
        def get(self, key):
            return params("dev")[key]
    class Dbu:
        widgets = Widgets()
    assert job_parameters_from_widgets(Dbu())["environment"] == "dev"


@pytest.mark.parametrize("env", ["dev", "test"])
def test_runtime_config_contains_only_selected_environment(env):
    settings = json.loads((ROOT / "runtime_config/config.json").read_text())
    ctx = load_context_from_job(params(env))
    assert ctx.runtime_config == settings[env]
    assert ctx.runtime_config.get("catalog", "") == settings[env]["catalog"]
    assert ctx.runtime_config.get("missing", "fallback") == "fallback"


def test_business_modules_import_without_spark_or_framework_setup():
    import ast
    import runpy
    from src.framework.registry import PIPELINE_SCRIPTS

    for stages in PIPELINE_SCRIPTS.values():
        for relative in stages.values():
            path = ROOT / relative
            assert callable(runpy.run_path(str(path))["run"])
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.ImportFrom):
                    assert not (node.module or "").startswith("src.framework")


def test_runner_requires_run_entrypoint(tmp_path, monkeypatch):
    import sys
    from types import SimpleNamespace
    from src.framework import registry

    (tmp_path / "business.py").write_text("value = 1")
    monkeypatch.setitem(registry.PIPELINE_SCRIPTS, "example", {"bronze": "business.py"})
    monkeypatch.setitem(sys.modules, "src.framework.spark_helpers", SimpleNamespace(get_spark=lambda: object()))
    ctx = PipelineContext("dev", str(tmp_path), "example", "bronze")
    with pytest.raises(ValueError, match=r"must define run\(ctx\)"):
        PipelineRunner(FakeDbutils()).run(ctx)
