"""Publish the gold layer for the dashboard: parquet tables, query results and a run report in web/data/."""
import json
from datetime import datetime, timezone

import duckdb

from clinicalflow.config import DQ_REPORT, ROOT, WAREHOUSE
from clinicalflow.warehouse.load_duckdb import GOLD_TABLES, SQL_DIR

WEB_DATA = ROOT / "web" / "data"


def rows(con, sql):
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def run(timings=None):
    WEB_DATA.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(WAREHOUSE), read_only=True) as con:
        for name in GOLD_TABLES:
            con.execute(f"COPY {name} TO '{WEB_DATA / name}.parquet' (FORMAT parquet, COMPRESSION zstd)")
        insights = {f.stem: rows(con, f.read_text()) for f in sorted(SQL_DIR.glob("*.sql"))}
        counts = {n: con.execute(f"SELECT COUNT(*) FROM {n}").fetchone()[0] for n in GOLD_TABLES}
    (WEB_DATA / "insights.json").write_text(json.dumps(insights, indent=1, default=str))
    dq = json.loads(DQ_REPORT.read_text()) if DQ_REPORT.exists() else {}
    report = {
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "timings_sec": timings or {},
        "layer_stats": dq.get("stats"),
        "gold_row_counts": counts,
        "quality": dq.get("results"),
    }
    (WEB_DATA / "run_report.json").write_text(json.dumps(report, indent=1, default=str))
    print(f"exported {len(GOLD_TABLES)} tables + insights + run report to {WEB_DATA}")


if __name__ == "__main__":
    run()
