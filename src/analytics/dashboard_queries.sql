-- Queries behind the AI/BI dashboard. Paste each one as a dataset in
-- Dashboards > Create, replacing telco_dev with your catalog.

-- 1. Network operations: worst cells yesterday by drop rate (min 20 calls)
SELECT cell_id, city, technology,
       sum(call_attempts) AS calls,
       round(100 * sum(dropped_calls) / sum(call_attempts), 2) AS drop_rate_pct,
       round(avg(avg_prb_utilization_pct), 1) AS avg_prb_pct,
       sum(alarm_count) AS alarms
FROM telco_dev.gold.cell_kpi_hourly
WHERE hour_ts >= date_sub((SELECT max(hour_ts) FROM telco_dev.gold.cell_kpi_hourly), 1)
GROUP BY ALL
HAVING sum(call_attempts) >= 20
ORDER BY drop_rate_pct DESC
LIMIT 15;

-- 2. Revenue: estimated month-to-date revenue by plan
SELECT b.bill_month, p.plan_name,
       count(DISTINCT b.customer_id) AS customers,
       round(sum(b.estimated_total_eur), 2) AS est_revenue_eur,
       round(sum(b.voice_overage_eur + b.data_overage_eur), 2) AS overage_eur
FROM telco_dev.gold.monthly_bill_estimate b
JOIN telco_dev.silver.dim_plan p USING (plan_id)
GROUP BY ALL
ORDER BY bill_month DESC, est_revenue_eur DESC;

-- 3. Churn: does network experience relate to churn?
SELECT CASE WHEN drop_rate_30d_pct IS NULL THEN 'no calls'
            WHEN drop_rate_30d_pct < 2 THEN '< 2%'
            WHEN drop_rate_30d_pct < 5 THEN '2-5%'
            ELSE '>= 5%' END AS drop_rate_band,
       count(*) AS customers,
       round(100 * avg(CAST(is_churned AS INT)), 2) AS churn_rate_pct
FROM telco_dev.gold.customer_churn_features
GROUP BY ALL
ORDER BY drop_rate_band;

-- 4. Data quality trend from the DQ gate
SELECT date(checked_at) AS run_date, check_name, max(observed) AS observed, min(CAST(passed AS INT)) = 1 AS passed
FROM telco_dev.ops.dq_results
GROUP BY ALL
ORDER BY run_date DESC, check_name;

-- 5. Why were CDRs rejected?
SELECT rule, count(*) AS rejected
FROM telco_dev.silver.cdrs_quarantine
LATERAL VIEW explode(failed_rules) AS rule
GROUP BY rule
ORDER BY rejected DESC;

-- 6. Pipeline observability: expectation pass/fail from the event log
SELECT timestamp, details:flow_progress.data_quality.expectations AS expectations
FROM event_log(TABLE(telco_dev.silver.cdrs))
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC
LIMIT 50;
