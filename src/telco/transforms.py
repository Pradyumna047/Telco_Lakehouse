"""Transformation logic shared by the pipeline and the unit tests.

Nothing in here imports `dlt`, so every function can be tested with a local
SparkSession. The pipeline files in src/pipelines/ wire these into tables.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# ---------------------------------------------------------------- data quality rules
# name -> SQL boolean expression. Used for both `expect_all_or_drop` and the
# quarantine table, so the two can never drift apart.

CDR_RULES: dict[str, str] = {
    "valid_caller_msisdn": "caller_msisdn IS NOT NULL AND caller_msisdn RLIKE '^\\\\+49[1-9][0-9]{9,11}$'",
    "valid_record_type": "record_type IN ('VOICE', 'SMS', 'DATA')",
    "non_negative_duration": "duration_sec >= 0",
    "non_negative_volume": "data_volume_mb >= 0",
    "start_ts_present": "start_ts IS NOT NULL",
    "start_ts_not_future": "start_ts <= _ingested_at + INTERVAL 1 DAY",
}

# Warnings only: the row is kept but the metric shows up in the event log.
CDR_WARNINGS: dict[str, str] = {
    "known_cell": "cell_known",
}

CUSTOMER_RULES: dict[str, str] = {
    "customer_id_present": "customer_id IS NOT NULL",
    "valid_operation": "operation IN ('INSERT', 'UPDATE', 'DELETE')",
    "sequence_present": "sequence_num IS NOT NULL",
}

NETWORK_RULES: dict[str, str] = {
    "cell_present": "cell_id IS NOT NULL",
    "prb_in_range": "prb_utilization_pct IS NULL OR prb_utilization_pct BETWEEN 0 AND 100",
    "latency_positive": "latency_ms IS NULL OR latency_ms > 0",
}


def all_rules(rules: dict[str, str]) -> str:
    """Single SQL predicate that is true when every rule passes."""
    return " AND ".join(f"({expr})" for expr in rules.values())


def failed_rules_col(rules: dict[str, str]) -> Column:
    """Array of the rule names a row violates (NULL-safe: NULL counts as a failure)."""
    checks = [F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(name)) for name, expr in rules.items()]
    return F.array_compact(F.array(*checks))


# ---------------------------------------------------------------- cleansing

def normalize_msisdn(col: Column) -> Column:
    """Normalise German mobile numbers to E.164 (+49...).

    Handles '+49 151 1234567', '0151-1234567', '0049151...' and '+49151...'.
    Anything that is not recognisably a number comes back unchanged so the
    quality rule can reject it (we never silently invent a value).
    """
    digits = F.regexp_replace(col, r"[\s\-/()]", "")
    return (
        F.when(digits.rlike(r"^\+49\d+$"), digits)
        .when(digits.rlike(r"^0049\d+$"), F.concat(F.lit("+"), F.substring(digits, 3, 100)))
        .when(digits.rlike(r"^0[1-9]\d+$"), F.concat(F.lit("+49"), F.substring(digits, 2, 100)))
        .otherwise(F.nullif(F.trim(col), F.lit("")))
    )


def clean_cdrs(df: DataFrame) -> DataFrame:
    """Type-cast and standardise raw CDRs. Expects bronze columns."""
    return (
        df.withColumn("start_ts", F.to_timestamp("start_ts"))
        .withColumn("duration_sec", F.col("duration_sec").cast("int"))
        .withColumn("data_volume_mb", F.col("data_volume_mb").cast("double"))
        .withColumn("record_type", F.upper(F.trim("record_type")))
        .withColumn("caller_msisdn", normalize_msisdn(F.col("caller_msisdn")))
        .withColumn("callee_msisdn", normalize_msisdn(F.col("callee_msisdn")))
        .withColumn("is_dropped", F.col("call_result") == F.lit("DROPPED"))
        .withColumn("usage_date", F.to_date("start_ts"))
    )


def clean_customers(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("activated_at", F.to_timestamp("activated_at"))
        .withColumn("date_of_birth", F.to_date("date_of_birth"))
        .withColumn("sequence_num", F.col("sequence_num").cast("long"))
        .withColumn("operation", F.upper(F.trim("operation")))
        .withColumn("msisdn", normalize_msisdn(F.col("msisdn")))
        .withColumn("email", F.lower(F.trim("email")))
    )


def clean_network_events(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("prb_utilization_pct", F.col("prb_utilization_pct").cast("double"))
        .withColumn("active_users", F.col("active_users").cast("int"))
        .withColumn("latency_ms", F.col("latency_ms").cast("double"))
    )


# ---------------------------------------------------------------- business logic

def daily_usage(cdrs: DataFrame, keys: tuple[str, ...] = ("customer_id", "usage_date")) -> DataFrame:
    """Aggregate clean CDRs (must carry customer_id) to one row per `keys`."""
    return cdrs.groupBy(*keys).agg(
        F.round(F.sum(F.when(F.col("record_type") == "VOICE", F.col("duration_sec")).otherwise(0)) / 60, 2)
        .alias("voice_minutes"),
        F.sum(F.when(F.col("record_type") == "SMS", 1).otherwise(0)).alias("sms_count"),
        F.round(F.sum(F.when(F.col("record_type") == "DATA", F.col("data_volume_mb")).otherwise(0)), 2)
        .alias("data_mb"),
        F.sum(F.when(F.col("record_type") == "VOICE", 1).otherwise(0)).alias("voice_calls"),
        F.sum(F.col("is_dropped").cast("int")).alias("dropped_calls"),
        F.sum(F.col("roaming").cast("int")).alias("roaming_events"),
    )


def plan_days_per_month(dim_customer: DataFrame, months: DataFrame) -> DataFrame:
    """Days each customer spent on each plan per calendar month, from the SCD2 dimension.

    dim_customer: customer_id, plan_id, status, __START_AT, __END_AT (SCD2 output)
    months: bill_month (DATE, first of month)
    Only ACTIVE versions count. Open versions (__END_AT NULL) run to month end.
    """
    v = dim_customer.where(F.col("status") == "ACTIVE").select(
        "customer_id", "plan_id",
        F.to_date("__START_AT").alias("valid_from"),
        F.coalesce(F.to_date("__END_AT"), F.lit("9999-12-31").cast("date")).alias("valid_to"),
    )
    m = months.select("bill_month", F.last_day("bill_month").alias("month_end"))
    overlap_start = F.greatest(F.col("valid_from"), F.col("bill_month"))
    overlap_end = F.least(F.date_sub(F.col("valid_to"), 1), F.col("month_end"))
    return (
        v.join(m, (F.col("valid_from") <= F.col("month_end")) & (F.col("valid_to") > F.col("bill_month")))
        .withColumn("days", F.datediff(overlap_end, overlap_start) + 1)
        .where(F.col("days") > 0)
        .groupBy("customer_id", "plan_id", "bill_month")
        .agg(F.sum("days").alias("days_on_plan"))
    )


def estimate_monthly_bill(plan_days: DataFrame, usage: DataFrame, plans: DataFrame) -> DataFrame:
    """Rate monthly usage against the plan(s) the customer was on.

    plan_days: customer_id, plan_id, bill_month, days_on_plan
    usage:     customer_id, plan_id, bill_month, voice_minutes, data_mb (already rated to the
               plan valid at call time, i.e. a point-in-time join upstream)
    plans:     plan reference data

    The monthly fee is prorated by days on the plan. Included allowances are
    not prorated, which is a simplification real billing systems do handle.
    """
    u = usage.groupBy("customer_id", "plan_id", "bill_month").agg(
        F.sum("voice_minutes").alias("voice_minutes"),
        F.sum("data_mb").alias("data_mb"),
    )
    days_in_month = F.dayofmonth(F.last_day(F.col("bill_month")))
    j = (
        plan_days.join(u, ["customer_id", "plan_id", "bill_month"], "left")
        .fillna({"voice_minutes": 0.0, "data_mb": 0.0})
        .join(plans, "plan_id", "left")
    )
    extra_min = F.greatest(F.lit(0.0), F.col("voice_minutes") - F.col("included_minutes"))
    extra_gb = F.greatest(F.lit(0.0), F.col("data_mb") / 1024 - F.col("included_data_gb"))
    return j.select(
        "customer_id", "bill_month", "plan_id", "days_on_plan",
        F.round("voice_minutes", 2).alias("voice_minutes"),
        F.round(F.col("data_mb") / 1024, 3).alias("data_gb"),
        F.round(F.col("monthly_fee_eur") * F.col("days_on_plan") / days_in_month, 2).alias("prorated_fee_eur"),
        F.round(extra_min * F.col("price_per_extra_min_eur"), 2).alias("voice_overage_eur"),
        F.round(F.ceil(extra_gb) * F.col("price_per_extra_gb_eur"), 2).alias("data_overage_eur"),
    ).withColumn(
        "estimated_total_eur",
        F.round(F.col("prorated_fee_eur") + F.col("voice_overage_eur") + F.col("data_overage_eur"), 2),
    )
