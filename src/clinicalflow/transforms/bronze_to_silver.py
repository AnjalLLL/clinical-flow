"""Silver layer: typed, cleaned, de-duplicated tables. Rows that break a rule go to quarantine with a reason.

Direct identifiers in the raw data (SSN, names, address, passport) are dropped here and never reach silver.
"""
from functools import cache

from pyspark.sql import DataFrame, functions as F

from clinicalflow.config import BRONZE, QUARANTINE, SILVER

c = F.col

@cache
def specs():
    """Built on first use: Spark column expressions need an active SparkSession."""
    return {
        "patients": {
            "columns": {
                "patient_id": c("Id"),
                "birthdate": F.to_date("BIRTHDATE"),
                "deathdate": F.to_date("DEATHDATE"),
                "gender": F.upper(F.trim("GENDER")),
                "race": F.lower(F.trim("RACE")),
                "ethnicity": F.lower(F.trim("ETHNICITY")),
                "state": F.trim("STATE"),
                "county": F.trim("COUNTY"),
                "income": c("INCOME").cast("int"),
            },
            "key": ["patient_id"],
            "rules": {
                "null_patient_id": c("patient_id").isNull(),
                "null_birthdate": c("birthdate").isNull(),
                "death_before_birth": c("deathdate") < c("birthdate"),
            },
            "partition": None,
        },
        "encounters": {
            "columns": {
                "encounter_id": c("Id"),
                "patient_id": c("PATIENT"),
                "start_ts": F.to_timestamp("START"),
                "stop_ts": F.to_timestamp("STOP"),
                "encounter_class": F.lower(F.trim("ENCOUNTERCLASS")),
                "code": c("CODE"),
                "description": F.trim("DESCRIPTION"),
                "base_cost": c("BASE_ENCOUNTER_COST").cast("double"),
                "total_claim_cost": c("TOTAL_CLAIM_COST").cast("double"),
                "payer_coverage": c("PAYER_COVERAGE").cast("double"),
                "reason_code": c("REASONCODE"),
                "reason_description": F.trim("REASONDESCRIPTION"),
            },
            "key": ["encounter_id"],
            "rules": {
                "null_encounter_id": c("encounter_id").isNull(),
                "null_patient_id": c("patient_id").isNull(),
                "null_start": c("start_ts").isNull(),
                "stop_before_start": c("stop_ts") < c("start_ts"),
                "negative_cost": c("total_claim_cost") < 0,
            },
            "partition": "year",
            "year_from": "start_ts",
        },
        "conditions": {
            "columns": {
                "start_date": F.to_date("START"),
                "stop_date": F.to_date("STOP"),
                "patient_id": c("PATIENT"),
                "encounter_id": c("ENCOUNTER"),
                "code": c("CODE"),
                "description": F.trim("DESCRIPTION"),
            },
            "key": ["patient_id", "encounter_id", "code", "start_date"],
            "rules": {
                "null_patient_id": c("patient_id").isNull(),
                "null_code": c("code").isNull(),
                "null_start": c("start_date").isNull(),
            },
            "partition": None,
        },
        "medications": {
            "columns": {
                "start_ts": F.to_timestamp("START"),
                "stop_ts": F.to_timestamp("STOP"),
                "patient_id": c("PATIENT"),
                "encounter_id": c("ENCOUNTER"),
                "code": c("CODE"),
                "description": F.trim("DESCRIPTION"),
                "base_cost": c("BASE_COST").cast("double"),
                "dispenses": c("DISPENSES").cast("int"),
                "total_cost": c("TOTALCOST").cast("double"),
            },
            "key": ["patient_id", "encounter_id", "code", "start_ts"],
            "rules": {
                "null_patient_id": c("patient_id").isNull(),
                "null_start": c("start_ts").isNull(),
                "negative_cost": c("total_cost") < 0,
                "negative_dispenses": c("dispenses") < 0,
            },
            "partition": "year",
            "year_from": "start_ts",
        },
    }


def clean(df: DataFrame, table: str):
    """Return (silver_df, quarantine_df, stats) for one raw table."""
    spec = specs()[table]
    typed = df.select(*[expr.alias(name) for name, expr in spec["columns"].items()])
    if spec["partition"]:
        typed = typed.withColumn("year", F.year(spec["year_from"]))
    # first broken rule wins; a null comparison (e.g. missing stop date) counts as passing
    rules = list(spec["rules"].items())
    reason = F.when(rules[0][1], F.lit(rules[0][0]))
    for name, broken in rules[1:]:
        reason = reason.when(broken, F.lit(name))
    flagged = typed.withColumn("dq_reason", reason).cache()
    bad = flagged.filter(c("dq_reason").isNotNull())
    valid = flagged.filter(c("dq_reason").isNull()).drop("dq_reason")
    good = valid.dropDuplicates(spec["key"])
    total, n_bad, n_valid = flagged.count(), bad.count(), valid.count()
    n_good = good.count()
    stats = {"bronze": total, "quarantined": n_bad, "duplicates": n_valid - n_good, "silver": n_good}
    return good, bad, stats


def read_bronze(spark, table):
    return spark.read.option("header", True).csv(str(BRONZE / table))


def run(spark):
    stats = {}
    for table, spec in specs().items():
        good, bad, stats[table] = clean(read_bronze(spark, table), table)
        writer = good.write.mode("overwrite")
        if spec["partition"]:
            writer = writer.partitionBy(spec["partition"])
        writer.parquet(str(SILVER / table))
        bad.write.mode("overwrite").parquet(str(QUARANTINE / table))
        print(f"silver.{table}: {stats[table]}")
    return stats
