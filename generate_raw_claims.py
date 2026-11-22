"""
generate_raw_claims.py
-------------------------
Generates raw billing-claims CSV data — the input the Glue ETL job reads
from S3's `raw/claims/` prefix. Includes a few deliberately messy rows
(a duplicate claim_id, a missing billed_amount) so the ETL job's
cleaning/deduplication logic has something real to do.

Output: local_demo/data/raw_claims.csv
"""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path

import numpy as np

RNG = np.random.default_rng(5)
PAYERS = ["P01", "P02", "P03", "P04"]
STATUSES = ["Paid", "Denied", "Pending"]

OUT_PATH = Path(__file__).parent / "data" / "raw_claims.csv"
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)


def main():
    rows = []
    start = date(2026, 8, 1)
    for i in range(200):
        billed = round(float(RNG.gamma(3, 400)), 2)
        paid = round(billed * RNG.uniform(0.4, 0.9), 2) if RNG.random() > 0.2 else ""
        rows.append({
            "claim_id": f"CLM{i:05d}",
            "payer_code": RNG.choice(PAYERS),
            "service_date": (start + timedelta(days=int(RNG.integers(0, 45)))).isoformat(),
            "billed_amount": billed,
            "paid_amount": paid,
            "claim_status": RNG.choice(STATUSES),
        })

    # Inject a duplicate claim_id and a missing billed_amount for the ETL job to clean up
    rows.append(dict(rows[10]))
    bad_row = dict(rows[50])
    bad_row["billed_amount"] = ""
    rows.append(bad_row)

    with open(OUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {OUT_PATH} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
