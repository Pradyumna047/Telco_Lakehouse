"""One-time (idempotent) Unity Catalog setup: catalog, schemas, landing volume,
governance objects and grants.

Run by the `telco_setup` job. Safe to re-run.
"""

import argparse

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

p = argparse.ArgumentParser()
p.add_argument("--catalog", required=True)
p.add_argument("--analyst-group", default="telco_analysts")
p.add_argument("--pii-group", default="telco_pii_readers")
args = p.parse_args()
C = args.catalog


def run(sql: str, optional: bool = False) -> None:
    print(sql.strip().splitlines()[0])
    try:
        spark.sql(sql)
    except Exception as e:  # noqa: BLE001
        if not optional:
            raise
        print(f"  skipped: {str(e).splitlines()[0][:200]}")


# Free Edition: creating a catalog normally works. If your workspace does not
# allow it, create the catalog once in Catalog Explorer, or point the bundle's
# `catalog` variable at the existing `workspace` catalog.
run(f"CREATE CATALOG IF NOT EXISTS {C} COMMENT 'Telco lakehouse portfolio project'", optional=True)

for schema, comment in [
    ("landing", "Raw file drops (volumes only)"),
    ("bronze", "Raw, append-only tables loaded by Auto Loader"),
    ("silver", "Cleansed, conformed tables and dimensions"),
    ("gold", "Business marts and governed views"),
    ("ops", "Pipeline operations: data quality results, audit"),
]:
    run(f"CREATE SCHEMA IF NOT EXISTS {C}.{schema} COMMENT '{comment}'")

run(f"CREATE VOLUME IF NOT EXISTS {C}.landing.raw COMMENT 'Landing zone for CRM, mediation and OSS files'")

run(f"""
CREATE TABLE IF NOT EXISTS {C}.ops.dq_results (
  run_id STRING, checked_at TIMESTAMP, check_name STRING, table_name STRING,
  observed DOUBLE, threshold DOUBLE, passed BOOLEAN, details STRING
) COMMENT 'One row per data quality check per job run'
""")

# ------------------------------------------------------------- PII masking
# Mask functions return the clear value only to members of the PII group.
run(f"""
CREATE OR REPLACE FUNCTION {C}.gold.mask_msisdn(msisdn STRING)
RETURNS STRING
COMMENT 'Shows only the last 3 digits unless the caller is in {args.pii_group}'
RETURN CASE WHEN is_account_group_member('{args.pii_group}') THEN msisdn
            ELSE concat(repeat('*', greatest(length(msisdn) - 3, 0)), right(msisdn, 3)) END
""")
run(f"""
CREATE OR REPLACE FUNCTION {C}.gold.mask_email(email STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('{args.pii_group}') THEN email
            ELSE concat(left(email, 1), '***@', split_part(email, '@', 2)) END
""")

# Governed view for analysts: current customers, PII masked, DOB reduced to an age band.
# Fails on the very first run because silver.dim_customer does not exist until the
# pipeline has run once; the daily job re-applies it after the pipeline (see resources/jobs.yml).
run(f"""
CREATE OR REPLACE VIEW {C}.gold.v_customer_current
COMMENT 'Current customers for analytics. PII masked for non-privileged users.'
AS SELECT
  customer_id,
  {C}.gold.mask_msisdn(msisdn) AS msisdn,
  {C}.gold.mask_email(email)   AS email,
  CASE WHEN is_account_group_member('{args.pii_group}') THEN concat(first_name, ' ', last_name)
       ELSE NULL END AS full_name,
  CASE
    WHEN floor(months_between(current_date(), date_of_birth) / 12) < 25 THEN '18-24'
    WHEN floor(months_between(current_date(), date_of_birth) / 12) < 40 THEN '25-39'
    WHEN floor(months_between(current_date(), date_of_birth) / 12) < 60 THEN '40-59'
    ELSE '60+' END AS age_band,
  city, region, plan_id, status, activated_at, __START_AT AS current_since
FROM {C}.silver.dim_customer
WHERE __END_AT IS NULL
""", optional=True)

# ------------------------------------------------------------- grants
# Groups are created by an account/workspace admin (Settings > Identity and access).
# On Free Edition you are the admin, so create them there first, or these are skipped.
g = args.analyst_group
for stmt in [
    f"GRANT USE CATALOG ON CATALOG {C} TO `{g}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.gold TO `{g}`",
    f"GRANT USE CATALOG ON CATALOG {C} TO `{args.pii_group}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.gold TO `{args.pii_group}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {C}.silver TO `{args.pii_group}`",
]:
    run(stmt, optional=True)

print("setup complete")
