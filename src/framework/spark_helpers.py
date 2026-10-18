"""Spark helpers shared across independent business notebooks."""
import re
from pyspark.sql import SparkSession


def get_spark():
    # In a Databricks notebook Spark is already available; do not start Databricks Connect.
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _identifier(value: str) -> str:
    """Validate and quote a Unity Catalog identifier (including names starting with digits)."""
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_]+", value):
        raise ValueError(f"Invalid Unity Catalog identifier: {value!r}")
    return f"`{value}`"


def table_name(ctx, schema_key: str, table: str) -> str:
    return ".".join(_identifier(part) for part in (ctx.require("citibike_catalog"), ctx.require(schema_key), table))


def ensure_schema(spark, ctx, schema_key: str) -> None:
    schema_name = ".".join(_identifier(part) for part in (ctx.require("citibike_catalog"), ctx.require(schema_key)))
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def volume_file_path(ctx) -> str:
    """Volume name and filename are regular data-path components, not SQL identifiers."""
    parts = [ctx.require(key) for key in ("citibike_catalog", "landing_schema", "source_volume", "source_file")]
    for part in parts:
        if not isinstance(part, str) or not part or part in (".", "..") or "/" in part or "\\" in part:
            raise ValueError(f"Unsafe Unity Catalog volume path component: {part!r}")
    return "/Volumes/" + "/".join(parts)
