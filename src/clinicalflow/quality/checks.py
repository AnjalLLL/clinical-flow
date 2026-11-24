"""Table-level data quality checks on silver and gold. Results are written to lake/dq_report.json."""
import json

from pyspark.sql import DataFrame, functions as F

from clinicalflow.config import DQ_REPORT, GOLD, SILVER


def check(name, failed_rows):
    return {"check": name, "failed_rows": int(failed_rows), "passed": failed_rows == 0}


def not_null(df: DataFrame, col):
    return check(f"not_null:{col}", df.filter(F.col(col).isNull()).count())


def unique(df: DataFrame, cols):
    dupes = df.groupBy(*cols).count().filter("count > 1").count()
    return check(f"unique:{','.join(cols)}", dupes)


def non_negative(df: DataFrame, col):
    return check(f"non_negative:{col}", df.filter(F.col(col) < 0).count())


def references(child: DataFrame, parent: DataFrame, col, label):
    orphans = child.join(parent.select(col).distinct(), col, "left_anti").count()
    return check(f"references:{label}", orphans)


def reconciles(stats):
    """bronze rows == silver + quarantined + duplicates, so no row is lost without a reason."""
    lost = stats["bronze"] - stats["silver"] - stats["quarantined"] - stats["duplicates"]
    return check("row_count_reconciliation", abs(lost))


def silver_checks(t, stats=None):
    """t: dict of silver DataFrames."""
    results = {
        "patients": [not_null(t["patients"], "patient_id"), unique(t["patients"], ["patient_id"])],
        "encounters": [
            not_null(t["encounters"], "encounter_id"),
            unique(t["encounters"], ["encounter_id"]),
            non_negative(t["encounters"], "total_claim_cost"),
            references(t["encounters"], t["patients"], "patient_id", "encounters.patient_id->patients"),
        ],
        "conditions": [
            not_null(t["conditions"], "code"),
            references(t["conditions"], t["patients"], "patient_id", "conditions.patient_id->patients"),
        ],
        "medications": [
            non_negative(t["medications"], "total_cost"),
            references(t["medications"], t["encounters"], "encounter_id", "medications.encounter_id->encounters"),
        ],
    }
    for table, s in (stats or {}).items():
        results[table].append(reconciles(s))
    return results


def gold_checks(g):
    return {
        "dim_patient": [unique(g["dim_patient"], ["patient_id"])],
        "dim_date": [unique(g["dim_date"], ["date_key"])],
        "fact_encounter": [
            unique(g["fact_encounter"], ["encounter_id"]),
            references(g["fact_encounter"], g["dim_patient"], "patient_id", "fact_encounter->dim_patient"),
            references(g["fact_encounter"], g["dim_date"], "date_key", "fact_encounter->dim_date"),
        ],
        "fact_medication": [
            references(g["fact_medication"], g["dim_date"], "date_key", "fact_medication->dim_date"),
        ],
    }


def failures(report):
    return [f"{layer}.{table}: {r['check']} ({r['failed_rows']} rows)"
            for layer, tables in report.items() for table, rs in tables.items() for r in rs if not r["passed"]]


def run(spark, stats=None):
    read = lambda base, names: {n: spark.read.parquet(str(base / n)) for n in names}  # noqa: E731
    silver = read(SILVER, ("patients", "encounters", "conditions", "medications"))
    gold = read(GOLD, ("dim_patient", "dim_date", "dim_condition", "fact_encounter", "fact_medication"))
    report = {"silver": silver_checks(silver, stats), "gold": gold_checks(gold)}
    DQ_REPORT.parent.mkdir(parents=True, exist_ok=True)
    DQ_REPORT.write_text(json.dumps({"stats": stats, "results": report}, indent=2))
    failed = failures(report)
    total = sum(len(rs) for tables in report.values() for rs in tables.values())
    print(f"data quality: {total - len(failed)}/{total} checks passed")
    return failed
