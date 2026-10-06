# Gold: business-facing marts. All are materialized views, so serverless
# pipelines can refresh them incrementally where the query shape allows it.
#
#   customer_daily_usage   usage per customer, day and the plan valid at call time
#   monthly_bill_estimate  rated usage + prorated fees (finance / revenue assurance)
#   cell_kpi_hourly        network quality per cell and hour (network operations)
#   customer_churn_features one row per customer, input for a churn model (marketing / data science)

import sys

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.append(spark.conf.get("bundle.sourcePath", "."))  # noqa: F821

from telco.transforms import daily_usage, estimate_monthly_bill, plan_days_per_month  # noqa: E402

CATALOG = spark.conf.get("telco.catalog")  # noqa: F821
S, G = f"{CATALOG}.silver", f"{CATALOG}.gold"


def _customer_versions():
    """SCD2 versions with the first version opened back to the beginning of time,
    so late CDRs from before the initial CRM load still resolve to a customer."""
    w = Window.partitionBy("customer_id").orderBy("__START_AT")
    return (
        spark.read.table(f"{S}.dim_customer")  # noqa: F821
        .withColumn("_rn", F.row_number().over(w))
        .withColumn("valid_from", F.when(F.col("_rn") == 1, F.lit("1900-01-01").cast("timestamp"))
                    .otherwise(F.col("__START_AT")))
        .withColumn("valid_to", F.coalesce("__END_AT", F.lit("9999-12-31").cast("timestamp")))
        .select("customer_id", "msisdn", "plan_id", "valid_from", "valid_to")
    )


@dlt.table(
    name=f"{G}.customer_daily_usage",
    comment="Daily usage per customer, attributed to the tariff plan valid when the usage happened.",
    cluster_by=["usage_date"],
)
def customer_daily_usage():
    cdrs = spark.read.table(f"{S}.cdrs").alias("c")  # noqa: F821
    v = _customer_versions().alias("v")
    resolved = cdrs.join(
        v,
        (F.col("c.caller_msisdn") == F.col("v.msisdn"))
        & (F.col("c.start_ts") >= F.col("v.valid_from"))
        & (F.col("c.start_ts") < F.col("v.valid_to")),
        "inner",
    ).select("c.*", "v.customer_id", "v.plan_id")
    return daily_usage(resolved, keys=("customer_id", "plan_id", "usage_date"))


@dlt.table(
    name=f"{G}.monthly_bill_estimate",
    comment="Estimated monthly charge per customer and plan. Not an invoice: no taxes, discounts or credits.",
)
def monthly_bill_estimate():
    usage = spark.read.table(f"{G}.customer_daily_usage").withColumn(  # noqa: F821
        "bill_month", F.trunc("usage_date", "month")
    )
    months = usage.select("bill_month").distinct()
    plan_days = plan_days_per_month(spark.read.table(f"{S}.dim_customer"), months)  # noqa: F821
    plans = spark.read.table(f"{S}.dim_plan")  # noqa: F821
    return estimate_monthly_bill(plan_days, usage, plans)


@dlt.table(
    name=f"{G}.cell_kpi_hourly",
    comment="Per cell and hour: call attempts, drop rate, PRB load, latency, alarms.",
    cluster_by=["hour_ts"],
)
def cell_kpi_hourly():
    calls = (
        spark.read.table(f"{S}.cdrs")  # noqa: F821
        .where("record_type = 'VOICE' AND cell_known")
        .groupBy("cell_id", F.date_trunc("hour", "start_ts").alias("hour_ts"))
        .agg(F.count("*").alias("call_attempts"), F.sum(F.col("is_dropped").cast("int")).alias("dropped_calls"))
    )
    net = (
        spark.read.table(f"{S}.network_events")  # noqa: F821
        .groupBy("cell_id", F.date_trunc("hour", "event_ts").alias("hour_ts"))
        .agg(
            F.round(F.avg("prb_utilization_pct"), 2).alias("avg_prb_utilization_pct"),
            F.round(F.avg("latency_ms"), 1).alias("avg_latency_ms"),
            F.max("active_users").alias("peak_active_users"),
            F.sum(F.when(F.col("event_type") == "ALARM", 1).otherwise(0)).alias("alarm_count"),
            F.sum(F.when(F.col("severity") == "CRITICAL", 1).otherwise(0)).alias("critical_alarm_count"),
        )
    )
    cells = spark.read.table(f"{S}.dim_cell")  # noqa: F821
    return (
        net.join(calls, ["cell_id", "hour_ts"], "full_outer")
        .join(cells, "cell_id", "left")
        .fillna({"call_attempts": 0, "dropped_calls": 0, "alarm_count": 0, "critical_alarm_count": 0})
        .withColumn(
            "drop_rate_pct",
            F.when(F.col("call_attempts") > 0, F.round(100 * F.col("dropped_calls") / F.col("call_attempts"), 2)),
        )
    )


@dlt.table(
    name=f"{G}.customer_churn_features",
    comment=(
        "One row per customer with behavioural features as of the latest usage date. "
        "Label = current status CHURNED. For model training, rebuild as point-in-time snapshots "
        "before the churn date, otherwise post-churn inactivity leaks into the features."
    ),
)
def customer_churn_features():
    usage = spark.read.table(f"{G}.customer_daily_usage")  # noqa: F821
    as_of = usage.agg(F.max("usage_date").alias("as_of_date"))
    u = usage.crossJoin(as_of).withColumn("age_days", F.datediff("as_of_date", "usage_date"))

    def window_sum(col, lo, hi):
        return F.sum(F.when((F.col("age_days") >= lo) & (F.col("age_days") < hi), F.col(col)).otherwise(0))

    feats = u.groupBy("customer_id", "as_of_date").agg(
        F.round(window_sum("voice_minutes", 0, 7), 1).alias("voice_min_last_7d"),
        F.round(window_sum("voice_minutes", 7, 14), 1).alias("voice_min_prev_7d"),
        F.round(window_sum("data_mb", 0, 7) / 1024, 2).alias("data_gb_last_7d"),
        F.round(window_sum("data_mb", 7, 14) / 1024, 2).alias("data_gb_prev_7d"),
        window_sum("voice_calls", 0, 30).alias("voice_calls_30d"),
        window_sum("dropped_calls", 0, 30).alias("dropped_calls_30d"),
        F.min("age_days").alias("days_since_last_activity"),
    ).withColumn(
        "drop_rate_30d_pct",
        F.when(F.col("voice_calls_30d") > 0, F.round(100 * F.col("dropped_calls_30d") / F.col("voice_calls_30d"), 2)),
    )

    current = (
        spark.read.table(f"{S}.dim_customer")  # noqa: F821
        .where("__END_AT IS NULL")
        .select("customer_id", "plan_id", "region", "status", "activated_at")
    )
    plans = spark.read.table(f"{S}.dim_plan").select("plan_id", "plan_type", "monthly_fee_eur")  # noqa: F821
    plan_changes = (
        spark.read.table(f"{S}.dim_customer")  # noqa: F821
        .groupBy("customer_id").agg((F.countDistinct("plan_id") - 1).alias("plan_changes"))
    )
    return (
        current.crossJoin(as_of)
        .join(feats.drop("as_of_date"), "customer_id", "left")
        .join(plans, "plan_id", "left")
        .join(plan_changes, "customer_id", "left")
        .withColumn("tenure_days", F.datediff(F.col("as_of_date"), F.to_date("activated_at")))
        .withColumn("is_churned", F.col("status") == "CHURNED")
        .drop("activated_at")
    )
