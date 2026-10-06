"""Synthetic data generator for a German mobile operator.

Produces the four feeds an operator's data platform typically receives:

* reference data (tariff plans, radio cells) as CSV, written once
* customer CDC events from the CRM (INSERT / UPDATE / DELETE) as JSON lines
* call detail records (CDRs) from the mediation layer as JSON lines
* network KPI samples and alarms from the OSS as JSON lines

Each call to ``generate_batch`` advances a simulated clock and appends new files
to the landing zone, so repeated runs look like a daily file drop. State (the
customer base and the clock) is kept in ``_state/state.json`` inside the
landing directory, which makes runs resumable.

Data quality problems are injected on purpose so the pipeline has something to
catch: duplicate CDRs, malformed MSISDNs, negative durations, unknown cells,
late-arriving and future-dated records, and out-of-order CDC events.

Pure Python only (no Spark, no pandas) so it runs on serverless and in CI.
"""

from __future__ import annotations

import csv
import json
import os
import random
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta

BACKFILL_DAYS = 30  # first run starts this many days before today

PLANS = [
    # plan_id, name, type, monthly fee EUR, incl. minutes, incl. data GB, EUR/extra min, EUR/extra GB
    ("P_PRE_S", "Prepaid S", "PREPAID", 9.99, 200, 5, 0.09, 2.99),
    ("P_PRE_M", "Prepaid M", "PREPAID", 14.99, 500, 15, 0.09, 2.99),
    ("P_POST_M", "Unlimited Talk M", "POSTPAID", 29.99, 99999, 25, 0.0, 4.99),
    ("P_POST_L", "Unlimited Talk L", "POSTPAID", 44.99, 99999, 60, 0.0, 4.99),
    ("P_POST_XL", "Unlimited Max", "POSTPAID", 69.99, 99999, 99999, 0.0, 0.0),
]

# (region, city, lat, lon)
CITIES = [
    ("Baden-Wuerttemberg", "Stuttgart", 48.7758, 9.1829),
    ("Baden-Wuerttemberg", "Karlsruhe", 49.0069, 8.4037),
    ("Bayern", "Muenchen", 48.1351, 11.5820),
    ("Bayern", "Nuernberg", 49.4521, 11.0767),
    ("Berlin", "Berlin", 52.5200, 13.4050),
    ("Hamburg", "Hamburg", 53.5511, 9.9937),
    ("Hessen", "Frankfurt am Main", 50.1109, 8.6821),
    ("Nordrhein-Westfalen", "Koeln", 50.9375, 6.9603),
    ("Nordrhein-Westfalen", "Duesseldorf", 51.2277, 6.7735),
    ("Mecklenburg-Vorpommern", "Wismar", 53.8925, 11.4650),
    ("Sachsen", "Leipzig", 51.3397, 12.3731),
]

FIRST_NAMES = ["Lukas", "Anna", "Leon", "Mia", "Finn", "Emma", "Paul", "Hannah", "Jonas", "Lea",
               "Elias", "Sofia", "Noah", "Marie", "Ben", "Lina", "Arjun", "Priya", "Mehmet", "Elif"]
LAST_NAMES = ["Mueller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker",
              "Schulz", "Hoffmann", "Koch", "Richter", "Klein", "Wolf", "Yilmaz", "Sharma"]

ALARM_CODES = ["RF_DEGRADED", "BACKHAUL_LOSS", "POWER_FAIL", "HIGH_INTERFERENCE"]


@dataclass
class GeneratorConfig:
    initial_customers: int = 2000
    hours_per_batch: int = 24
    cdrs_per_customer_per_day: float = 12.0
    new_customer_rate: float = 0.004      # share of base added per day
    plan_change_rate: float = 0.003
    address_change_rate: float = 0.001
    base_churn_rate: float = 0.002
    # injected data quality issues (probability per record)
    p_duplicate_cdr: float = 0.01
    p_bad_msisdn: float = 0.004
    p_negative_duration: float = 0.003
    p_unknown_cell: float = 0.002
    p_late_cdr: float = 0.01
    p_future_cdr: float = 0.001
    p_out_of_order_cdc: float = 0.05
    cells_per_city: int = 6
    bad_cell_share: float = 0.08          # cells with poor radio quality
    # Simulated time starts BACKFILL_DAYS before today, so ~30 backfill batches
    # catch up with the real clock and a daily job then tracks real time. This
    # matters because the pipeline rejects records dated in the future.
    start_date: datetime | None = None
    allow_future: bool = False


@dataclass
class Customer:
    customer_id: str
    msisdn: str
    first_name: str
    last_name: str
    email: str
    date_of_birth: str
    city: str
    region: str
    plan_id: str
    home_cell_id: str
    status: str = "ACTIVE"
    activated_at: str = ""
    churn_affinity: float = 1.0     # hidden; multiplied into churn probability
    dropped_calls: int = 0          # hidden running counter that drives churn


@dataclass
class State:
    clock: str = ""
    batch_no: int = 0
    next_customer_seq: int = 1
    cdc_sequence: int = 0
    customers: dict[str, dict] = field(default_factory=dict)
    cells: list[dict] = field(default_factory=list)


# ---------------------------------------------------------------- helpers

def _iso(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _messy_msisdn(e164: str, rng: random.Random) -> str:
    """Render one E.164 number the way different source systems do."""
    national = "0" + e164[3:]
    style = rng.random()
    if style < 0.5:
        return e164
    if style < 0.7:
        return f"+49 {e164[3:6]} {e164[6:]}"
    if style < 0.85:
        return f"{national[:4]}-{national[4:]}"
    return "00" + e164[1:]


def _corrupt_msisdn(rng: random.Random) -> str | None:
    return rng.choice([None, "", "unknown", "+49123", "0000000000000000"])


def _build_cells(cfg: GeneratorConfig, rng: random.Random) -> list[dict]:
    cells = []
    for region, city, lat, lon in CITIES:
        for i in range(cfg.cells_per_city):
            cells.append({
                "cell_id": f"C-{city[:3].upper()}-{i:03d}",
                "site_id": f"S-{city[:3].upper()}-{i // 3:02d}",
                "technology": "5G" if rng.random() < 0.45 else "4G",
                "region": region,
                "city": city,
                "latitude": round(lat + rng.uniform(-0.05, 0.05), 5),
                "longitude": round(lon + rng.uniform(-0.05, 0.05), 5),
                "is_degraded": rng.random() < cfg.bad_cell_share,  # hidden, not exported
            })
    return cells


def _new_customer(state: State, cfg: GeneratorConfig, rng: random.Random, at: datetime) -> Customer:
    seq = state.next_customer_seq
    state.next_customer_seq += 1
    region, city, *_ = rng.choice(CITIES)
    city_cells = [c for c in state.cells if c["city"] == city]
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    dob = datetime(1950, 1, 1) + timedelta(days=rng.randint(0, 365 * 55))
    return Customer(
        customer_id=f"CUST{seq:07d}",
        msisdn=f"+491{rng.choice(['51', '52', '57', '60', '70', '76'])}{rng.randint(1000000, 9999999)}",
        first_name=first,
        last_name=last,
        email=f"{first.lower()}.{last.lower()}{seq}@example.de",
        date_of_birth=dob.date().isoformat(),
        city=city,
        region=region,
        plan_id=rng.choices([p[0] for p in PLANS], weights=[25, 20, 30, 18, 7])[0],
        home_cell_id=rng.choice(city_cells)["cell_id"],
        activated_at=_iso(at),
        churn_affinity=rng.uniform(0.3, 2.5),
    )


def _cdc_event(c: Customer, op: str, at: datetime, state: State) -> dict:
    state.cdc_sequence += 1
    return {
        "customer_id": c.customer_id,
        "msisdn": c.msisdn,
        "first_name": c.first_name,
        "last_name": c.last_name,
        "email": c.email,
        "date_of_birth": c.date_of_birth,
        "city": c.city,
        "region": c.region,
        "plan_id": c.plan_id,
        "status": c.status,
        "activated_at": c.activated_at,
        "operation": op,
        "event_ts": _iso(at),
        "sequence_num": state.cdc_sequence,
        "source_system": "CRM_SIEBEL",
    }


def _write_jsonl(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def _load_state(landing: str) -> State | None:
    path = os.path.join(landing, "_state", "state.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return State(**json.load(fh))


def _save_state(landing: str, state: State) -> None:
    path = os.path.join(landing, "_state", "state.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(asdict(state), fh)
    os.replace(tmp, path)


def _write_reference(landing: str, state: State) -> None:
    ref = os.path.join(landing, "reference")
    os.makedirs(os.path.join(ref, "plans"), exist_ok=True)
    os.makedirs(os.path.join(ref, "cells"), exist_ok=True)
    with open(os.path.join(ref, "plans", "plans.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["plan_id", "plan_name", "plan_type", "monthly_fee_eur", "included_minutes",
                    "included_data_gb", "price_per_extra_min_eur", "price_per_extra_gb_eur"])
        w.writerows(PLANS)
    cols = ["cell_id", "site_id", "technology", "region", "city", "latitude", "longitude"]
    with open(os.path.join(ref, "cells", "cells.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(state.cells)


# ---------------------------------------------------------------- main entry

def generate_batch(landing: str, cfg: GeneratorConfig | None = None, seed: int | None = None) -> dict:
    """Generate one batch of files into ``landing``. Returns a summary dict."""
    cfg = cfg or GeneratorConfig()
    state = _load_state(landing)
    first_run = state is None
    if first_run:
        today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        state = State(clock=(cfg.start_date or today - timedelta(days=BACKFILL_DAYS)).isoformat())
    rng = random.Random(seed if seed is not None else state.batch_no * 7919 + 17)

    if first_run:
        state.cells = _build_cells(cfg, rng)
        _write_reference(landing, state)

    start = datetime.fromisoformat(state.clock)
    end = start + timedelta(hours=cfg.hours_per_batch)
    if end > datetime.now(UTC) and not cfg.allow_future:
        return {"batch_id": None, "skipped": True,
                "reason": f"simulated clock ({_iso(start)}) has caught up with real time; nothing to generate"}
    days = cfg.hours_per_batch / 24
    batch_id = f"{start:%Y%m%dT%H%M}_{state.batch_no:05d}"
    cells_by_id = {c["cell_id"]: c for c in state.cells}
    customers = {k: Customer(**v) for k, v in state.customers.items()}

    def rand_ts() -> datetime:
        return start + timedelta(seconds=rng.uniform(0, cfg.hours_per_batch * 3600))

    # ---------------- customer CDC
    cdc: list[dict] = []
    if first_run:
        for _ in range(cfg.initial_customers):
            c = _new_customer(state, cfg, rng, start - timedelta(days=rng.randint(30, 2000)))
            customers[c.customer_id] = c
            cdc.append(_cdc_event(c, "INSERT", start, state))
    else:
        active = [c for c in customers.values() if c.status == "ACTIVE"]
        for _ in range(max(1, int(len(active) * cfg.new_customer_rate * days))):
            ts = rand_ts()
            c = _new_customer(state, cfg, rng, ts)
            customers[c.customer_id] = c
            cdc.append(_cdc_event(c, "INSERT", ts, state))
        for c in active:
            r = rng.random()
            # churn probability rises with dropped calls the customer suffered
            p_churn = cfg.base_churn_rate * days * c.churn_affinity * (1 + 0.15 * min(c.dropped_calls, 20))
            if r < p_churn:
                c.status = "CHURNED"
                cdc.append(_cdc_event(c, "UPDATE", rand_ts(), state))
            elif r < p_churn + cfg.plan_change_rate * days:
                c.plan_id = rng.choice([p[0] for p in PLANS if p[0] != c.plan_id])
                cdc.append(_cdc_event(c, "UPDATE", rand_ts(), state))
            elif r < p_churn + (cfg.plan_change_rate + cfg.address_change_rate) * days:
                region, city, *_ = rng.choice(CITIES)
                c.city, c.region = city, region
                c.home_cell_id = rng.choice([x for x in state.cells if x["city"] == city])["cell_id"]
                cdc.append(_cdc_event(c, "UPDATE", rand_ts(), state))
        # GDPR erasure requests: a few long-churned customers get hard-deleted
        churned = [c for c in customers.values() if c.status == "CHURNED"]
        for c in rng.sample(churned, k=min(len(churned), 1 if rng.random() < 0.3 else 0)):
            cdc.append(_cdc_event(c, "DELETE", rand_ts(), state))
            del customers[c.customer_id]
        # out-of-order delivery: shuffle a slice of the feed
        cdc.sort(key=lambda e: e["sequence_num"])
        for i in range(len(cdc) - 1):
            if rng.random() < cfg.p_out_of_order_cdc:
                cdc[i], cdc[i + 1] = cdc[i + 1], cdc[i]

    # ---------------- CDRs
    cdrs: list[dict] = []
    active = [c for c in customers.values() if c.status == "ACTIVE"]
    msisdns = [c.msisdn for c in active]
    stats = {"duplicates": 0, "bad_msisdn": 0, "negative_duration": 0, "unknown_cell": 0,
             "late": 0, "future": 0}
    for c in active:
        mean = cfg.cdrs_per_customer_per_day * days * c.churn_affinity ** -0.3
        n = max(0, round(rng.gauss(mean, mean ** 0.5)))
        for _ in range(n):
            ts = rand_ts()
            cell = cells_by_id[c.home_cell_id] if rng.random() < 0.7 else rng.choice(state.cells)
            rtype = rng.choices(["VOICE", "SMS", "DATA"], weights=[45, 15, 40])[0]
            result = "COMPLETED"
            if rtype == "VOICE":
                p_drop = 0.12 if cell["is_degraded"] else 0.015
                u = rng.random()
                result = "DROPPED" if u < p_drop else ("FAILED" if u < p_drop + 0.01 else "COMPLETED")
                if result == "DROPPED":
                    c.dropped_calls += 1
            rec = {
                "cdr_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "record_type": rtype,
                "caller_msisdn": _messy_msisdn(c.msisdn, rng),
                "callee_msisdn": _messy_msisdn(rng.choice(msisdns), rng) if rtype != "DATA" else None,
                "start_ts": _iso(ts),
                "duration_sec": int(rng.expovariate(1 / 140)) + 1 if rtype == "VOICE" else
                                (int(rng.expovariate(1 / 600)) + 1 if rtype == "DATA" else 0),
                "data_volume_mb": round(rng.expovariate(1 / 45), 3) if rtype == "DATA" else 0.0,
                "cell_id": cell["cell_id"],
                "call_result": result if rtype == "VOICE" else "COMPLETED",
                "roaming": rng.random() < 0.02,
                "source_system": rng.choice(["MED_ERICSSON", "MED_NOKIA"]),
            }
            if rng.random() < cfg.p_bad_msisdn:
                rec["caller_msisdn"] = _corrupt_msisdn(rng)
                stats["bad_msisdn"] += 1
            if rtype == "VOICE" and rng.random() < cfg.p_negative_duration:
                rec["duration_sec"] = -rng.randint(1, 500)
                stats["negative_duration"] += 1
            if rng.random() < cfg.p_unknown_cell:
                rec["cell_id"] = f"C-XXX-{rng.randint(900, 999)}"
                stats["unknown_cell"] += 1
            if rng.random() < cfg.p_late_cdr:
                rec["start_ts"] = _iso(ts - timedelta(hours=rng.randint(25, 60)))
                stats["late"] += 1
            elif rng.random() < cfg.p_future_cdr:
                rec["start_ts"] = _iso(end + timedelta(days=rng.randint(30, 400)))
                stats["future"] += 1
            cdrs.append(rec)
            if rng.random() < cfg.p_duplicate_cdr:
                cdrs.append(dict(rec))  # mediation re-delivery
                stats["duplicates"] += 1
    rng.shuffle(cdrs)

    # ---------------- network KPI samples (15 min) and alarms
    net: list[dict] = []
    t = start
    while t < end:
        for cell in state.cells:
            degraded = cell["is_degraded"]
            base_util = 35 + 30 * (1 if 8 <= t.hour <= 22 else 0) + (20 if degraded else 0)
            net.append({
                "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "cell_id": cell["cell_id"],
                "event_ts": _iso(t),
                "event_type": "KPI_SAMPLE",
                "prb_utilization_pct": round(min(100.0, max(0.0, rng.gauss(base_util, 10))), 2),
                "active_users": max(0, int(rng.gauss(120 if 8 <= t.hour <= 22 else 30, 20))),
                "latency_ms": round(max(5.0, rng.gauss(55 if cell["is_degraded"] else 28, 8)), 1),
                "alarm_code": None,
                "severity": None,
            })
            if rng.random() < (0.02 if cell["is_degraded"] else 0.001):
                net.append({
                    "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                    "cell_id": cell["cell_id"],
                    "event_ts": _iso(t + timedelta(minutes=rng.randint(0, 14))),
                    "event_type": "ALARM",
                    "prb_utilization_pct": None,
                    "active_users": None,
                    "latency_ms": None,
                    "alarm_code": rng.choice(ALARM_CODES),
                    "severity": rng.choice(["MINOR", "MAJOR", "CRITICAL"]),
                })
        t += timedelta(minutes=15)
    # sensor glitch: a few impossible readings
    for row in rng.sample(net, k=min(len(net), 5)):
        if row["event_type"] == "KPI_SAMPLE":
            row["prb_utilization_pct"] = rng.choice([-1.0, 140.0])

    day = f"{start:%Y-%m-%d}"
    _write_jsonl(os.path.join(landing, "customers", f"ingest_date={day}", f"crm_customers_{batch_id}.json"), cdc)
    _write_jsonl(os.path.join(landing, "cdrs", f"ingest_date={day}", f"cdr_{batch_id}.json"), cdrs)
    _write_jsonl(os.path.join(landing, "network_events", f"ingest_date={day}", f"oss_{batch_id}.json"), net)

    state.customers = {k: asdict(v) for k, v in customers.items()}
    state.clock = end.isoformat()
    state.batch_no += 1
    _save_state(landing, state)

    return {
        "batch_id": batch_id,
        "window_start": _iso(start),
        "window_end": _iso(end),
        "customer_events": len(cdc),
        "cdrs": len(cdrs),
        "network_events": len(net),
        "active_customers": len([c for c in customers.values() if c.status == "ACTIVE"]),
        "injected": stats,
    }
