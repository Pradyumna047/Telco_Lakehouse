from datetime import UTC, date, datetime

import pytest
from pyspark.sql import functions as F

from telco.transforms import (
    CDR_RULES,
    all_rules,
    clean_cdrs,
    daily_usage,
    estimate_monthly_bill,
    failed_rules_col,
    normalize_msisdn,
    plan_days_per_month,
)

UTC = UTC


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("+491511234567", "+491511234567"),
        ("+49 151 1234567", "+491511234567"),
        ("0151-1234567", "+491511234567"),
        ("00491511234567", "+491511234567"),
        ("unknown", "unknown"),   # left as-is so the rule rejects it
        ("", None),
        (None, None),
    ],
)
def test_normalize_msisdn(spark, raw, expected):
    df = spark.createDataFrame([(raw,)], "m STRING")
    assert df.select(normalize_msisdn(F.col("m"))).first()[0] == expected


def _raw_cdr(**over):
    row = {
        "cdr_id": "a", "record_type": "VOICE", "caller_msisdn": "0151-1234567",
        "callee_msisdn": "+491701234567", "start_ts": "2026-09-01T10:00:00.000Z",
        "duration_sec": "60", "data_volume_mb": "0.0", "cell_id": "C-STU-000",
        "call_result": "COMPLETED", "roaming": False, "source_system": "MED_NOKIA",
        "_ingested_at": datetime(2026, 9, 2, 1, 0, tzinfo=UTC),
    }
    row.update(over)
    return row


SCHEMA = ("cdr_id STRING, record_type STRING, caller_msisdn STRING, callee_msisdn STRING, start_ts STRING, "
          "duration_sec STRING, data_volume_mb STRING, cell_id STRING, call_result STRING, roaming BOOLEAN, "
          "source_system STRING, _ingested_at TIMESTAMP")


def _rows(spark, *rows):
    cols = [c.split()[0] for c in SCHEMA.split(", ")]
    return spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], SCHEMA)


def test_cdr_rules_pass_good_row_and_name_failures(spark):
    df = clean_cdrs(_rows(
        spark,
        _raw_cdr(cdr_id="good"),
        _raw_cdr(cdr_id="neg", duration_sec="-5"),
        _raw_cdr(cdr_id="nocaller", caller_msisdn=None),
        _raw_cdr(cdr_id="future", start_ts="2027-06-01T00:00:00.000Z"),
    )).withColumn("failed", failed_rules_col(CDR_RULES))
    out = {r.cdr_id: set(r.failed) for r in df.collect()}
    assert out["good"] == set()
    assert out["neg"] == {"non_negative_duration"}
    assert out["nocaller"] == {"valid_caller_msisdn"}
    assert out["future"] == {"start_ts_not_future"}

    passing = df.where(all_rules(CDR_RULES)).select("cdr_id").collect()
    assert [r.cdr_id for r in passing] == ["good"]


def test_daily_usage_aggregates(spark):
    df = clean_cdrs(_rows(
        spark,
        _raw_cdr(cdr_id="1", duration_sec="120"),
        _raw_cdr(cdr_id="2", duration_sec="60", call_result="DROPPED"),
        _raw_cdr(cdr_id="3", record_type="DATA", duration_sec="600", data_volume_mb="100.5"),
        _raw_cdr(cdr_id="4", record_type="SMS", duration_sec="0"),
    )).withColumn("customer_id", F.lit("CUST1"))
    r = daily_usage(df).first()
    assert r.voice_minutes == 3.0
    assert r.voice_calls == 2 and r.dropped_calls == 1
    assert r.sms_count == 1
    assert r.data_mb == 100.5


def test_plan_days_split_on_plan_change(spark):
    dim = spark.createDataFrame(
        [
            ("C1", "P_A", "ACTIVE", datetime(2026, 9, 1, tzinfo=UTC), datetime(2026, 9, 11, tzinfo=UTC)),
            ("C1", "P_B", "ACTIVE", datetime(2026, 9, 11, tzinfo=UTC), None),
        ],
        "customer_id STRING, plan_id STRING, status STRING, __START_AT TIMESTAMP, __END_AT TIMESTAMP",
    )
    months = spark.createDataFrame([(date(2026, 9, 1),)], "bill_month DATE")
    got = {r.plan_id: r.days_on_plan for r in plan_days_per_month(dim, months).collect()}
    assert got == {"P_A": 10, "P_B": 20}


def test_churned_version_not_billed(spark):
    dim = spark.createDataFrame(
        [
            ("C1", "P_A", "ACTIVE", datetime(2026, 9, 1, tzinfo=UTC), datetime(2026, 9, 16, tzinfo=UTC)),
            ("C1", "P_A", "CHURNED", datetime(2026, 9, 16, tzinfo=UTC), None),
        ],
        "customer_id STRING, plan_id STRING, status STRING, __START_AT TIMESTAMP, __END_AT TIMESTAMP",
    )
    months = spark.createDataFrame([(date(2026, 9, 1),)], "bill_month DATE")
    assert plan_days_per_month(dim, months).first().days_on_plan == 15


def test_bill_estimate_prorates_and_rates_overage(spark):
    plans = spark.createDataFrame(
        [("P_A", 30.0, 100, 1, 0.10, 5.0)],
        "plan_id STRING, monthly_fee_eur DOUBLE, included_minutes INT, included_data_gb INT, "
        "price_per_extra_min_eur DOUBLE, price_per_extra_gb_eur DOUBLE",
    )
    plan_days = spark.createDataFrame([("C1", "P_A", date(2026, 9, 1), 15)],
                                      "customer_id STRING, plan_id STRING, bill_month DATE, days_on_plan INT")
    usage = spark.createDataFrame(
        [("C1", "P_A", date(2026, 9, 1), date(2026, 9, 5), 150.0, 2.5 * 1024)],
        "customer_id STRING, plan_id STRING, bill_month DATE, usage_date DATE, voice_minutes DOUBLE, data_mb DOUBLE",
    )
    r = estimate_monthly_bill(plan_days, usage, plans).first()
    assert r.prorated_fee_eur == 15.0          # 30 EUR * 15/30 days
    assert r.voice_overage_eur == 5.0          # 50 min * 0.10
    assert r.data_overage_eur == 10.0          # ceil(1.5 GB) * 5
    assert r.estimated_total_eur == 30.0


def test_customer_without_usage_still_pays_fee(spark):
    plans = spark.createDataFrame(
        [("P_A", 31.0, 100, 1, 0.10, 5.0)],
        "plan_id STRING, monthly_fee_eur DOUBLE, included_minutes INT, included_data_gb INT, "
        "price_per_extra_min_eur DOUBLE, price_per_extra_gb_eur DOUBLE",
    )
    plan_days = spark.createDataFrame([("C9", "P_A", date(2026, 10, 1), 31)],
                                      "customer_id STRING, plan_id STRING, bill_month DATE, days_on_plan INT")
    usage = spark.createDataFrame([], "customer_id STRING, plan_id STRING, bill_month DATE, usage_date DATE, "
                                      "voice_minutes DOUBLE, data_mb DOUBLE")
    r = estimate_monthly_bill(plan_days, usage, plans).first()
    assert r.estimated_total_eur == 31.0
