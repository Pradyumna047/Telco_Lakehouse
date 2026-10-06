# Build plan: 3 weeks

The code in this repo is a working scaffold. The plan below is about getting
it running on your workspace, understanding every part well enough to defend it,
and adding the pieces that turn it into *your* project. Each week ends with
something you can show.

## Week 1: get it running end to end

| Day | Task | Done when |
|---|---|---|
| 1 | Read `generator.py` and run `tools/local_pipeline_run.py`. Open the landing files and find each injected defect by hand. | You can name all 7 defect types and where each is handled. |
| 2 | Set up Free Edition, CLI auth, push the repo to GitHub. `bundle validate`, `bundle deploy -t dev`, run `telco_setup`. | Catalog with 5 schemas and the landing volume exists. |
| 3 | Run `telco_daily` with `batches=3`. Read the pipeline graph and the expectation metrics in the pipeline UI. | All tables populated; you can explain why `cdrs_validated` drops rows. |
| 4 | Query `dim_customer`: find a customer with a plan change and walk through `__START_AT/__END_AT`. Then backfill with `batches=27`. | You can explain SCD1 vs SCD2 using your own data. |
| 5 | Run the DQ gate, look at `ops.dq_results`. Break something on purpose (e.g. set `p_duplicate_cdr=0` and remove the dedup) and watch the gate fail. | Screenshot of a failed run with the alert e-mail. |

## Week 2: make it yours

Pick at least two. These are the parts that are *not* written for you.

1. **Dashboard.** Build an AI/BI dashboard from `src/analytics/dashboard_queries.sql`
   with three pages: network, revenue, data quality. Add it to the bundle
   (`resources/dashboard.yml`, `file_path` to an exported `.lvdash.json`).
2. **Streaming.** Switch the pipeline to `continuous: true` for an hour, run the
   generator with `--hours-per-batch 1` in a loop, and observe latency. Write down
   what changed in cost and freshness.
3. **GDPR purge job.** On a DELETE event, remove the customer's PII from bronze and
   silver history (MERGE/DELETE plus `VACUUM`), and log the request in `ops`.
   This is a real requirement in every German telco project.
4. **Liquid clustering and performance.** Compare query times on `silver.cdrs`
   with and without `cluster_by`. Use the query profile, not guesses.
5. **Row filter.** Add a regional row filter so a `telco_analysts_bw` group only
   sees Baden-Wuerttemberg customers.
6. **Schema evolution.** Add a field (e.g. `imei`) to the generator mid-stream and
   handle it: see where it lands (`_rescued_data`) and promote it to a column.

## Week 3: production habits and the write-up

| Task | Notes |
|---|---|
| CI green on GitHub | Add `DATABRICKS_HOST` (variable) and `DATABRICKS_TOKEN` (secret). PRs run tests and `bundle validate`; `main` deploys to dev. |
| Deploy `prod` target | Same workspace on Free Edition, different catalog. Unpause the schedule there only. |
| Runbook | One page in `docs/`: what to do when the DQ gate fails, how to backfill, how to full-refresh one table. |
| Cost note | From `system.billing.usage` if available on your workspace, otherwise from the job run durations. |
| README results | Replace the design section's claims with your numbers: rows/day, reject rate, pipeline duration, the churn-vs-drop-rate chart. |
| Short demo video | 3 minutes: the defect, how the pipeline catches it, where it shows up. |

## Stretch: ML on top (if time is left)

Train a churn classifier on point-in-time snapshots (not the current table, see
README limits), log it with MLflow, register it in Unity Catalog, and write
scores back to `gold.churn_scores` with a batch job.

## Questions you should be able to answer after this

- Why does the quarantine table read from bronze and not from the validated view?
- What happens to a CDR that arrives 4 days late? Where would you see it?
- Why are gold tables materialized views and silver tables streaming tables?
- How would this change if the CRM sent full snapshots instead of CDC?
- Where does this design break at 1 billion CDRs per day, and what would you change?
