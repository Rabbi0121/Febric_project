# Fabric notebook-compatible PySpark orchestrator template.
# Real-data mode: no synthetic/demo fallback.

from __future__ import annotations

import glob
import os
import shutil
import sys
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

# Make local execution work even when IDE/shell does not export Java vars.
os.environ.setdefault(
    "JAVA_HOME", "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
)
os.environ["PATH"] = f"/opt/homebrew/opt/openjdk@17/bin:{os.environ.get('PATH', '')}"

# Force Spark driver and workers to use the same Python interpreter.
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg as f_avg
from pyspark.sql.functions import col, count as f_count, sum as f_sum, to_date
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("fabric-medallion-orchestrator").getOrCreate()

PROJECT_ROOT = Path(__file__).resolve().parent.parent



def _table_exists(name: str) -> bool:
    try:
        spark.table(name)
        return True
    except AnalysisException:
        return False



def _load_parquet_patterns(view_name: str, patterns: list[str]) -> bool:
    def _load_single_with_pandas(parquet_file: str, pattern: str) -> bool:
        try:
            import pandas as pd

            pandas_df = pd.read_parquet(parquet_file)
            spark.createDataFrame(pandas_df).createOrReplaceTempView(view_name)
            print(
                "Loaded "
                f"{view_name} via pandas compatibility fallback "
                f"(1 file(s), pattern='{pattern}')."
            )
            return True
        except Exception:
            return False

    for pattern in patterns:
        files = sorted(glob.glob(str(PROJECT_ROOT / pattern)))
        parquet_files = [f for f in files if f.endswith(".parquet")]
        if not parquet_files:
            continue

        # Weather snapshots can be encoded with timestamp precision that local Spark
        # cannot always infer; prefer pandas for this known single-file case.
        if view_name == "bronze_weather" and len(parquet_files) == 1:
            if _load_single_with_pandas(parquet_files[0], pattern):
                return True

        try:
            spark.read.parquet(*parquet_files).createOrReplaceTempView(view_name)
            print(
                "Loaded "
                f"{view_name} from local real parquet files "
                f"({len(parquet_files)} file(s), pattern='{pattern}')."
            )
            return True
        except Exception as spark_read_exc:
            # Fallback for compatibility edge-cases where Spark cannot infer specific
            # parquet encodings/types but pandas can still decode the file.
            if len(parquet_files) == 1:
                if _load_single_with_pandas(parquet_files[0], pattern):
                    return True

            print(
                "WARNING: Failed to load "
                f"{view_name} from pattern '{pattern}' ({type(spark_read_exc).__name__}: "
                f"{spark_read_exc}). Trying next fallback pattern."
            )

    return False



def _ensure_bronze_view(
    name: str,
    fallback_patterns: list[str],
    *,
    required: bool = True,
) -> bool:
    if _table_exists(name):
        return True
    if _load_parquet_patterns(name, fallback_patterns):
        return True

    if required:
        raise RuntimeError(
            f"Missing required source table/view '{name}'. "
            "Run upstream ingestion first (Fabric Bronze/Silver) or materialize the expected parquet files. "
            f"Checked patterns: {fallback_patterns}"
        )

    print(
        f"WARNING: Optional source table/view '{name}' is missing. "
        f"Checked patterns: {fallback_patterns}. Continuing without this dataset."
    )
    return False



def _warehouse_base_path() -> Path:
    warehouse_uri = spark.conf.get("spark.sql.warehouse.dir")
    parsed = urlparse(warehouse_uri)

    if parsed.scheme == "file":
        return Path(unquote(parsed.path))
    if not parsed.scheme:
        return Path(unquote(warehouse_uri))

    raise RuntimeError(f"Unsupported spark.sql.warehouse.dir URI scheme: {parsed.scheme}")



def _safe_save_managed_table(df, table_name: str) -> None:
    # Drop stale metadata and stale location so reruns remain stable locally.
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    table_dir = _warehouse_base_path() / table_name
    if table_dir.exists():
        shutil.rmtree(table_dir)
        print(f"Removed stale table location: {table_dir}")
    df.write.mode("overwrite").saveAsTable(table_name)



def _ensure_sources() -> dict[str, Any]:
    _ensure_bronze_view(
        "bronze_nyc_taxi",
        [
            "data/lakehouse/bronze/nyc_taxi/*.parquet",
            "data/lakehouse/silver/nyc_taxi/*.parquet",
        ],
        required=True,
    )
    openaq_available = _ensure_bronze_view(
        "bronze_openaq",
        [
            "data/lakehouse/bronze/openaq/*.parquet",
            "data/lakehouse/silver/openaq/*.parquet",
            "data/lakehouse/gold/openaq/*.parquet",
        ],
        required=False,
    )
    _ensure_bronze_view(
        "bronze_weather",
        [
            "data/lakehouse/bronze/weather/*.parquet",
            "data/lakehouse/silver/weather/*.parquet",
            "data/lakehouse/gold/weather/*.parquet",
        ],
        required=True,
    )
    return {"openaq_available": openaq_available}


source_status = _ensure_sources()

# Silver: standardization examples
silver_taxi = (
    spark.table("bronze_nyc_taxi")
    .select(
        "tpep_pickup_datetime",
        "PULocationID",
        "passenger_count",
        "trip_distance",
        "total_amount",
    )
    .dropDuplicates()
)

if source_status["openaq_available"]:
    silver_air = (
        spark.table("bronze_openaq")
        .select("timestamp_utc", "city", "parameter", "value", "unit")
        .dropDuplicates()
    )
else:
    silver_air = None

silver_weather = (
    spark.table("bronze_weather")
    .select(
        "timestamp",
        "city",
        "temperature_c",
        "humidity_pct",
        "precipitation_mm",
        "wind_speed_kmh",
    )
    .dropDuplicates()
)

# Gold facts
gold_taxi_daily = (
    silver_taxi.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
    .groupBy("pickup_date", "PULocationID")
    .agg(
        f_count("*").alias("trip_count"),
        f_sum("passenger_count").alias("passenger_total"),
        f_sum("total_amount").alias("revenue_usd"),
        f_avg("trip_distance").alias("avg_trip_distance_miles"),
    )
)

if silver_air is not None:
    gold_air_daily = (
        silver_air.withColumn("date", to_date(col("timestamp_utc")))
        .groupBy("date", "city", "parameter")
        .agg(
            f_avg("value").alias("avg_pollutant_value"),
            f_count("*").alias("measurement_count"),
        )
    )
else:
    gold_air_daily = None

gold_weather_daily = (
    silver_weather.withColumn("date", to_date(col("timestamp")))
    .groupBy("date", "city")
    .agg(
        f_avg("temperature_c").alias("avg_temperature_c"),
        f_sum("precipitation_mm").alias("total_precipitation_mm"),
        f_avg("humidity_pct").alias("avg_humidity_pct"),
        f_avg("wind_speed_kmh").alias("avg_wind_speed_kmh"),
    )
)

# Persist back into Lakehouse tables
_safe_save_managed_table(gold_taxi_daily, "gold_fact_taxi_daily")
if gold_air_daily is not None:
    _safe_save_managed_table(gold_air_daily, "gold_fact_air_quality_daily")
else:
    print("Skipped gold_fact_air_quality_daily because OpenAQ source was not available.")
_safe_save_managed_table(gold_weather_daily, "gold_fact_weather_daily")

print("Pipeline completed successfully in real-data mode. Gold tables were written.")
