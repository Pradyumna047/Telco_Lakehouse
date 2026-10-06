# Bronze: raw files from the landing volume, ingested incrementally with Auto Loader.
# No business logic here. Columns are kept as delivered, plus ingestion metadata,
# so any silver table can be rebuilt from bronze with a full refresh.

import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("telco.catalog")  # noqa: F821  (spark is injected by the pipeline runtime)
LANDING = spark.conf.get("telco.landing_path")  # noqa: F821


def _autoload(subdir: str, fmt: str, schema_hints: str):
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.inferColumnTypes", "false")  # keep raw values as strings
        .option("cloudFiles.schemaHints", schema_hints)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.partitionColumns", "")  # ignore ingest_date=... folder names
        .load(f"{LANDING}/{subdir}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dlt.table(
    name=f"{CATALOG}.bronze.cdrs",
    comment="Raw call detail records from mediation (Ericsson/Nokia), append-only.",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
    cluster_by=["_ingested_at"],
)
def bronze_cdrs():
    return _autoload("cdrs", "json", "cdr_id STRING, start_ts STRING, roaming BOOLEAN")


@dlt.table(
    name=f"{CATALOG}.bronze.customers_cdc",
    comment="Raw CDC feed from the CRM. One row per INSERT/UPDATE/DELETE event.",
    table_properties={"quality": "bronze"},
)
def bronze_customers_cdc():
    return _autoload("customers", "json", "customer_id STRING, sequence_num STRING")


@dlt.table(
    name=f"{CATALOG}.bronze.network_events",
    comment="Raw OSS feed: 15-minute cell KPI samples and alarms.",
    table_properties={"quality": "bronze"},
    cluster_by=["_ingested_at"],
)
def bronze_network_events():
    return _autoload("network_events", "json", "event_id STRING, cell_id STRING")


# Reference data is small and replaced in full, so a materialized view over the CSV is enough.
@dlt.table(name=f"{CATALOG}.bronze.ref_plans", comment="Tariff plan catalogue (CSV drop).")
def bronze_ref_plans():
    return spark.read.option("header", True).csv(f"{LANDING}/reference/plans")  # noqa: F821


@dlt.table(name=f"{CATALOG}.bronze.ref_cells", comment="Radio cell inventory (CSV drop).")
def bronze_ref_cells():
    return spark.read.option("header", True).csv(f"{LANDING}/reference/cells")  # noqa: F821
