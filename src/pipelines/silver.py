# Silver: cleansed, typed, deduplicated, conformed.
#  - CDRs: normalised MSISDNs, quality rules (drop + quarantine), dedup on cdr_id,
#    resolved to customer_id and enriched with cell attributes.
#  - Customers: SCD Type 2 dimension built from the CRM CDC feed (AUTO CDC / apply_changes).
#  - Network events: typed and validated.

import sys

import dlt
from pyspark.sql import functions as F

sys.path.append(spark.conf.get("bundle.sourcePath", "."))  # noqa: F821

from telco.transforms import (  # noqa: E402
    CDR_RULES,
    CDR_WARNINGS,
    CUSTOMER_RULES,
    NETWORK_RULES,
    all_rules,
    clean_cdrs,
    clean_customers,
    clean_network_events,
    failed_rules_col,
)

CATALOG = spark.conf.get("telco.catalog")  # noqa: F821
B, S = f"{CATALOG}.bronze", f"{CATALOG}.silver"


# ---------------------------------------------------------------- reference dimensions

@dlt.table(name=f"{S}.dim_plan", comment="Tariff plans, typed.")
@dlt.expect_or_fail("plan_id_present", "plan_id IS NOT NULL")
def dim_plan():
    return spark.read.table(f"{B}.ref_plans").select(  # noqa: F821
        "plan_id", "plan_name", "plan_type",
        F.col("monthly_fee_eur").cast("decimal(8,2)").alias("monthly_fee_eur"),
        F.col("included_minutes").cast("int").alias("included_minutes"),
        F.col("included_data_gb").cast("int").alias("included_data_gb"),
        F.col("price_per_extra_min_eur").cast("decimal(6,3)").alias("price_per_extra_min_eur"),
        F.col("price_per_extra_gb_eur").cast("decimal(6,2)").alias("price_per_extra_gb_eur"),
    )


@dlt.table(name=f"{S}.dim_cell", comment="Radio cells with site, technology and location.")
def dim_cell():
    return spark.read.table(f"{B}.ref_cells").select(  # noqa: F821
        "cell_id", "site_id", "technology", "region", "city",
        F.col("latitude").cast("double").alias("latitude"),
        F.col("longitude").cast("double").alias("longitude"),
    )


# ---------------------------------------------------------------- customers (SCD2)

@dlt.view(comment="Typed CRM CDC events that pass basic validation.")
@dlt.expect_all_or_drop(CUSTOMER_RULES)
def customers_cdc_clean():
    return clean_customers(spark.readStream.table(f"{B}.customers_cdc"))  # noqa: F821


dlt.create_streaming_table(
    name=f"{S}.dim_customer",
    comment="Customer dimension, SCD Type 2. Current row: __END_AT IS NULL.",
    table_properties={"quality": "silver"},
)

# apply_changes is the long-standing name; newer runtimes also expose it as
# dlt.create_auto_cdc_flow with the same arguments.
dlt.apply_changes(
    target=f"{S}.dim_customer",
    source="customers_cdc_clean",
    keys=["customer_id"],
    # Event time, not arrival order: files arrive shuffled. __START_AT/__END_AT take
    # the type of this column, so gold can do point-in-time joins on timestamps.
    sequence_by=F.col("event_ts"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),  # GDPR erasure
    except_column_list=["operation", "sequence_num", "source_system",
                        "_rescued_data", "_source_file", "_file_modified_at", "_ingested_at"],
    stored_as_scd_type=2,
    track_history_column_list=["plan_id", "status", "city", "region"],
)


# ---------------------------------------------------------------- CDRs

def _cdr_base():
    cells = spark.read.table(f"{S}.dim_cell").select("cell_id", "region", "technology")  # noqa: F821
    return (
        clean_cdrs(spark.readStream.table(f"{B}.cdrs"))  # noqa: F821
        .join(F.broadcast(cells), "cell_id", "left")
        .withColumn("cell_known", F.col("region").isNotNull())
    )


@dlt.view(comment="CDRs that pass every quality rule. Expectation metrics are recorded here.")
@dlt.expect_all_or_drop(CDR_RULES)
@dlt.expect_all(CDR_WARNINGS)
def cdrs_validated():
    # Rules run BEFORE the watermark on purpose: a single future-dated record
    # would otherwise push the watermark forward and every normal record after
    # it would be discarded as "late".
    return _cdr_base()


@dlt.table(
    name=f"{S}.cdrs",
    comment="Validated, deduplicated CDRs. One row per cdr_id.",
    table_properties={"quality": "silver"},
    cluster_by=["usage_date", "caller_msisdn"],
)
def silver_cdrs():
    return (
        spark.readStream.table("cdrs_validated")  # noqa: F821
        # mediation re-delivers the same record; 3 days covers the late feed we see
        .withWatermark("start_ts", "3 days")
        .dropDuplicatesWithinWatermark(["cdr_id"])
        .select(
            "cdr_id", "record_type", "caller_msisdn", "callee_msisdn", "start_ts", "usage_date",
            "duration_sec", "data_volume_mb", "call_result", "is_dropped", "roaming",
            "cell_id", "cell_known", "region", "technology", "source_system",
            "_source_file", "_ingested_at",
        )
    )


@dlt.table(
    name=f"{S}.cdrs_quarantine",
    comment="CDRs rejected by quality rules, with the reasons. Reviewed by the data owner.",
    table_properties={"quality": "quarantine"},
)
def silver_cdrs_quarantine():
    return (
        _cdr_base()
        .where(f"NOT ({all_rules(CDR_RULES)}) OR ({all_rules(CDR_RULES)}) IS NULL")
        .withColumn("failed_rules", failed_rules_col(CDR_RULES))
    )


# ---------------------------------------------------------------- network events

@dlt.table(
    name=f"{S}.network_events",
    comment="Typed OSS KPI samples and alarms.",
    table_properties={"quality": "silver"},
    cluster_by=["cell_id"],
)
@dlt.expect_all_or_drop(NETWORK_RULES)
def silver_network_events():
    return (
        clean_network_events(spark.readStream.table(f"{B}.network_events"))  # noqa: F821
        .withWatermark("event_ts", "1 day")
        .dropDuplicatesWithinWatermark(["event_id"])
        .drop("_rescued_data")
    )
