"""Post-pipeline data quality gate.

Runs after the pipeline in the daily job. Each check writes a row to
<catalog>.ops.dq_results; if any blocking check fails the task fails, the job
is marked failed, and the notification in resources/jobs.yml fires.

Pipeline expectations catch bad *rows*. These checks catch bad *tables*:
reconciliation gaps, broken SCD2 invariants, stale data, unusual reject rates.
"""

import argparse
import json
import uuid
from datetime import UTC, datetime

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

p = argparse.ArgumentParser()
p.add_argument("--catalog", required=True)
p.add_argument("--max-quarantine-pct", type=float, default=5.0)
p.add_argument("--max-unresolved-pct", type=float, default=2.0)
p.add_argument("--max-staleness-hours", type=float, default=48.0)
args = p.parse_args()
C = args.catalog

run_id = str(uuid.uuid4())
now = datetime.now(UTC)
results = []


def scalar(sql: str):
    return spark.sql(sql).collect()[0][0]


def check(name, table, observed, threshold, passed, blocking=True, details=None):
    observed = float(observed) if observed is not None else None
    results.append((run_id, now, name, table, observed, float(threshold), bool(passed),
                    json.dumps({"blocking": blocking, **(details or {})})))
    status = ("PASS" if passed else "FAIL") + ("" if blocking else " (warn)")
    print(f"[{status}] {name}: observed={observed} threshold={threshold}")


# 1. No duplicate CDRs survived dedup
dups = scalar(f"SELECT count(*) - count(DISTINCT cdr_id) FROM {C}.silver.cdrs")
check("silver_cdrs_unique_cdr_id", f"{C}.silver.cdrs", dups, 0, dups == 0)

# 2. Reject rate inside tolerance (bronze vs quarantine)
bronze = scalar(f"SELECT count(*) FROM {C}.bronze.cdrs")
quarantined = scalar(f"SELECT count(*) FROM {C}.silver.cdrs_quarantine")
q_pct = 100 * quarantined / bronze if bronze else 0
check("cdr_quarantine_pct", f"{C}.silver.cdrs_quarantine", q_pct, args.max_quarantine_pct,
      q_pct <= args.max_quarantine_pct, details={"bronze": bronze, "quarantined": quarantined})

# 3. Reconciliation: bronze = silver + quarantine + removed duplicates (+ watermark-dropped)
silver = scalar(f"SELECT count(*) FROM {C}.silver.cdrs")
bronze_distinct_valid = bronze - quarantined
gap = bronze_distinct_valid - silver
check("cdr_reconciliation_gap_rows", f"{C}.silver.cdrs", gap, max(1, bronze * 0.02), gap <= max(1, bronze * 0.02),
      blocking=False, details={"bronze": bronze, "silver": silver, "quarantined": quarantined,
                               "note": "gap = duplicates + records later than the watermark"})

# 4. SCD2 invariant: exactly one open version per active key
bad_scd = scalar(f"""
  SELECT count(*) FROM (
    SELECT customer_id FROM {C}.silver.dim_customer WHERE __END_AT IS NULL
    GROUP BY customer_id HAVING count(*) > 1)""")
check("dim_customer_single_current_row", f"{C}.silver.dim_customer", bad_scd, 0, bad_scd == 0)

# 5. CDRs that do not resolve to a customer (MSISDN not in CRM)
unresolved_pct = scalar(f"""
  SELECT 100 * avg(CASE WHEN d.customer_id IS NULL THEN 1 ELSE 0 END)
  FROM {C}.silver.cdrs c
  LEFT JOIN (SELECT DISTINCT msisdn, customer_id FROM {C}.silver.dim_customer) d
    ON c.caller_msisdn = d.msisdn""") or 0
check("cdr_unresolved_msisdn_pct", f"{C}.silver.cdrs", unresolved_pct, args.max_unresolved_pct,
      unresolved_pct <= args.max_unresolved_pct)

# 6. Freshness: the newest usage in gold is recent relative to the newest CDR
lag_h = scalar(f"""
  SELECT (unix_timestamp(max_cdr) - unix_timestamp(max_gold)) / 3600 FROM
   (SELECT max(start_ts) AS max_cdr FROM {C}.silver.cdrs WHERE start_ts <= current_timestamp() + INTERVAL 1 DAY)
   CROSS JOIN (SELECT cast(max(usage_date) AS TIMESTAMP) AS max_gold FROM {C}.gold.customer_daily_usage)""") or 0
check("gold_usage_freshness_hours", f"{C}.gold.customer_daily_usage", lag_h, args.max_staleness_hours,
      lag_h <= args.max_staleness_hours)

# 7. Billing sanity: no negative or absurd estimates
bad_bills = scalar(f"""SELECT count(*) FROM {C}.gold.monthly_bill_estimate
                       WHERE estimated_total_eur < 0 OR estimated_total_eur > 2000""")
check("bill_estimate_in_range", f"{C}.gold.monthly_bill_estimate", bad_bills, 0, bad_bills == 0, blocking=False)

spark.createDataFrame(results, schema=spark.table(f"{C}.ops.dq_results").schema) \
    .write.mode("append").saveAsTable(f"{C}.ops.dq_results")

failed = [r for r in results if not r[6] and json.loads(r[7])["blocking"]]
if failed:
    raise SystemExit(f"{len(failed)} blocking data quality check(s) failed: {[r[2] for r in failed]}")
print(f"all blocking checks passed (run_id={run_id})")
