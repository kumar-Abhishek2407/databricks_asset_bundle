"""Registered business scripts; add pipelines here, not in the runner."""

# Paths are relative to the deployed bundle root (${workspace.file_path}).
# Keeping an explicit allowlist avoids executing arbitrary paths from a job parameter.
PIPELINE_SCRIPTS: dict[str, dict[str, str]] = {
    "citibike_etl": {
        "bronze": "pipeline_modules/citibike_etl/bronze/bronze_citibike.py",
        "silver": "pipeline_modules/citibike_etl/silver/silver_citybike.py",
        "gold_summary": "pipeline_modules/citibike_etl/gold/gold_citibike_daily_ride_summary.py",
        "gold_performance": "pipeline_modules/citibike_etl/gold/gold_citibike_daily_station_performance.py",
    },
}
