import json
import os
from datetime import UTC, datetime

from telco.generator import GeneratorConfig, generate_batch

FIXED_START = datetime(2026, 9, 1, tzinfo=UTC)
SMALL = GeneratorConfig(initial_customers=50, cdrs_per_customer_per_day=4, cells_per_city=2, start_date=FIXED_START)


def _read_jsonl(folder):
    rows = []
    for root, _, files in os.walk(folder):
        for f in files:
            with open(os.path.join(root, f)) as fh:
                rows += [json.loads(line) for line in fh]
    return rows


def test_first_batch_writes_reference_and_feeds(tmp_path):
    summary = generate_batch(str(tmp_path), SMALL)
    assert (tmp_path / "reference" / "plans" / "plans.csv").exists()
    assert (tmp_path / "reference" / "cells" / "cells.csv").exists()
    assert summary["customer_events"] == 50
    assert summary["cdrs"] > 0 and summary["network_events"] > 0
    cdc = _read_jsonl(tmp_path / "customers")
    assert {e["operation"] for e in cdc} == {"INSERT"}


def test_clock_advances_and_state_resumes(tmp_path):
    a = generate_batch(str(tmp_path), SMALL)
    b = generate_batch(str(tmp_path), SMALL)
    assert b["window_start"] == a["window_end"]
    assert b["batch_id"] != a["batch_id"]


def test_cdc_sequence_strictly_increasing_across_batches(tmp_path):
    for _ in range(3):
        generate_batch(str(tmp_path), SMALL)
    seqs = [e["sequence_num"] for e in _read_jsonl(tmp_path / "customers")]
    assert len(seqs) == len(set(seqs))
    assert max(seqs) == len(seqs)


def test_hidden_columns_not_exported(tmp_path):
    generate_batch(str(tmp_path), SMALL)
    header = (tmp_path / "reference" / "cells" / "cells.csv").read_text().splitlines()[0]
    assert "is_degraded" not in header
    cdc = _read_jsonl(tmp_path / "customers")
    assert "churn_affinity" not in cdc[0] and "dropped_calls" not in cdc[0]


def test_quality_issues_are_injected(tmp_path):
    cfg = GeneratorConfig(initial_customers=300, p_duplicate_cdr=0.05, p_negative_duration=0.05,
                          start_date=FIXED_START)
    generate_batch(str(tmp_path), cfg)
    cdrs = _read_jsonl(tmp_path / "cdrs")
    ids = [c["cdr_id"] for c in cdrs]
    assert len(ids) > len(set(ids)), "expected duplicate cdr_ids"
    assert any((c["duration_sec"] or 0) < 0 for c in cdrs), "expected negative durations"


def test_stops_at_real_time(tmp_path):
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    cfg = GeneratorConfig(initial_customers=10, cells_per_city=1, start_date=today)
    assert generate_batch(str(tmp_path), cfg)["skipped"] is True
    assert not (tmp_path / "cdrs").exists()


def test_default_start_is_backfill_window(tmp_path):
    s = generate_batch(str(tmp_path), GeneratorConfig(initial_customers=10, cells_per_city=1))
    start = datetime.fromisoformat(s["window_start"].replace("Z", "+00:00"))
    assert 29 <= (datetime.now(UTC) - start).days <= 30
