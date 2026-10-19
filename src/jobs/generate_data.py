"""Job entry point: drop one or more batches of synthetic files into the landing volume.

Runs as a serverless `spark_python_task`. Locally:
    python src/jobs/generate_data.py --landing ./_landing --batches 3
"""

import argparse
import json
import os
import sys

# make `telco` importable both in the workspace (bundle files) and locally
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:  # serverless job tasks exec() the file without defining __file__
    import inspect
    _here = os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename))
sys.path.insert(0, os.path.abspath(os.path.join(_here, "..")))

from telco.generator import GeneratorConfig, generate_batch  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog")
    p.add_argument("--landing", help="override landing path (default /Volumes/<catalog>/landing/raw)")
    p.add_argument("--batches", type=int, default=1)
    p.add_argument("--initial-customers", type=int, default=2000)
    p.add_argument("--hours-per-batch", type=int, default=24)
    args = p.parse_args()

    landing = args.landing or f"/Volumes/{args.catalog}/landing/raw"
    cfg = GeneratorConfig(initial_customers=args.initial_customers, hours_per_batch=args.hours_per_batch)
    for _ in range(args.batches):
        print(json.dumps(generate_batch(landing, cfg)))


if __name__ == "__main__":
    main()
