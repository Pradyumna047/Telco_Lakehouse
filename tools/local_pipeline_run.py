"""Run the pipeline definitions end to end on a laptop, without Databricks.

A small stand-in for the `dlt` module executes every table/view in batch mode
with plain PySpark, applies the expectations, and builds the SCD2 dimension.
It is a smoke test for the transformation logic and table wiring, not a
replacement for running the real pipeline: streaming semantics, Auto Loader
checkpoints and incremental MV refresh are not simulated.

    python tools/local_pipeline_run.py --landing ./_landing --batches 5
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import types

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import DataFrame, SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from telco.generator import GeneratorConfig, generate_batch  # noqa: E402
from telco.transforms import all_rules  # noqa: E402

CATALOG = "telco_local"


# ------------------------------------------------------------------ fake dlt
class Registry:
    def __init__(self):
        self.steps: list[tuple] = []  # (kind, name, payload)
        self.tables: dict[str, DataFrame] = {}
        self.metrics: dict[str, dict[str, int]] = {}


REG = Registry()


def _attach(fn, key, value):
    setattr(fn, key, getattr(fn, key, []) + [value])
    return fn


def make_dlt_module() -> types.ModuleType:
    m = types.ModuleType("dlt")

    def table(name=None, **_):
        def deco(fn):
            REG.steps.append(("table", name or fn.__name__, fn))
            return fn
        return deco

    def view(name=None, **_):
        def deco(fn):
            REG.steps.append(("table", name or fn.__name__, fn))
            return fn
        return deco

    m.table, m.view = table, view
    m.expect_all_or_drop = lambda rules: (lambda fn: _attach(fn, "_drop", rules))
    m.expect_all = lambda rules: (lambda fn: _attach(fn, "_warn", rules))
    m.expect_or_fail = lambda name, expr: (lambda fn: _attach(fn, "_fail", {name: expr}))
    m.create_streaming_table = lambda name, **_: None
    m.apply_changes = lambda **kw: REG.steps.append(("scd2", kw["target"], kw))
    m.create_auto_cdc_flow = m.apply_changes
    return m


class ReaderShim:
    def __init__(self, spark: SparkSession):
        self._spark, self._fmt, self._opts = spark, None, {}

    def format(self, fmt):
        self._fmt = fmt
        return self

    def option(self, k, v):
        self._opts[k] = v
        return self

    def load(self, path):
        fmt = self._opts.get("cloudFiles.format", self._fmt)
        df = self._spark.read.format(fmt).option("recursiveFileLookup", "true").load(path)
        return df.select("*", "_metadata").withColumn("_rescued_data", F.lit(None).cast("string"))

    def table(self, name):
        return REG.tables[name]

    def csv(self, path):
        return self._spark.read.options(**self._opts).csv(path)

    def __getattr__(self, item):
        return getattr(self._spark.read, item)


class SparkShim:
    def __init__(self, spark, conf):
        self._spark, self._conf = spark, conf
        self.conf = types.SimpleNamespace(get=lambda k, d=None: conf.get(k, d))

    @property
    def readStream(self):  # noqa: N802
        return ReaderShim(self._spark)

    @property
    def read(self):
        return ReaderShim(self._spark)

    def __getattr__(self, item):
        return getattr(self._spark, item)


def build_scd2(kw: dict) -> DataFrame:
    src = REG.tables[kw["source"]]
    seq = kw["sequence_by"]
    keys = kw["keys"]
    w = Window.partitionBy(*keys).orderBy(seq)
    df = (
        src.withColumn("__seq", seq)
        .withColumn("__is_delete", kw["apply_as_deletes"])
        .withColumn("__START_AT", F.col("__seq"))
        .withColumn("__END_AT", F.lead("__seq").over(w))
        .where(~F.col("__is_delete"))
    )
    return df.drop("__seq", "__is_delete", *kw.get("except_column_list", []))


# Batch DataFrames reject dropDuplicatesWithinWatermark; in batch mode it is a plain dedup.
try:  # Spark 4 splits DataFrame into classic/connect implementations
    from pyspark.sql.classic.dataframe import DataFrame as _DF
except ImportError:
    _DF = DataFrame
_orig_dedup = _DF.dropDuplicatesWithinWatermark
_DF.dropDuplicatesWithinWatermark = (
    lambda self, subset=None: self.dropDuplicates(subset) if not self.isStreaming else _orig_dedup(self, subset)
)


def run_pipeline(spark: SparkSession, landing: str) -> None:
    conf = {"telco.catalog": CATALOG, "telco.landing_path": landing, "bundle.sourcePath": os.path.join(ROOT, "src")}
    sys.modules["dlt"] = make_dlt_module()
    shim = SparkShim(spark, conf)
    for f in ["bronze.py", "silver.py", "gold.py"]:
        path = os.path.join(ROOT, "src", "pipelines", f)
        exec(compile(open(path).read(), path, "exec"), {"spark": shim, "__name__": f})

    for kind, name, payload in REG.steps:
        if kind == "scd2":
            df = build_scd2(payload)
        else:
            df = payload()
            for rules in getattr(payload, "_fail", []):
                bad = df.where(f"NOT ({all_rules(rules)})").count()
                assert bad == 0, f"{name}: expect_or_fail violated by {bad} rows"
            for rules in getattr(payload, "_drop", []):
                before = df.count()
                df = df.where(all_rules(rules))
                REG.metrics[name] = {"input": before, "dropped": before - df.count()}
        REG.tables[name] = df.localCheckpoint()
        print(f"  {name:<45} {REG.tables[name].count():>9,} rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--landing", default=os.path.join(ROOT, "_landing"))
    ap.add_argument("--batches", type=int, default=5)
    ap.add_argument("--customers", type=int, default=500)
    ap.add_argument("--keep", action="store_true", help="keep existing landing data")
    args = ap.parse_args()

    if not args.keep and os.path.exists(args.landing):
        shutil.rmtree(args.landing)
    cfg = GeneratorConfig(initial_customers=args.customers)
    for _ in range(args.batches):
        s = generate_batch(args.landing, cfg)
        if s.get("skipped"):
            print(s["reason"])
            break
        print(f"generated {s['batch_id']}: {s['cdrs']:,} CDRs, {s['customer_events']} CRM events")

    spark = (SparkSession.builder.master("local[2]").appName("telco-local")
             .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    print("running pipeline:")
    run_pipeline(spark, args.landing)
    print("expectation drops:", REG.metrics)
    return spark


if __name__ == "__main__":
    main()
