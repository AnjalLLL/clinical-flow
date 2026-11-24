"""Gold layer: star schema built from silver.

dim_patient, dim_date, dim_condition, fact_encounter, fact_medication, fact_condition
"""
from pyspark.sql import functions as F

from clinicalflow.config import GOLD, SILVER

c = F.col


def date_key(col):
    return F.date_format(col, "yyyyMMdd").cast("int")


def build(silver):
    """silver: dict of table name -> DataFrame. Returns dict of gold table name -> DataFrame."""
    pat, enc, cond, med = (silver[t] for t in ("patients", "encounters", "conditions", "medications"))

    age = F.floor(F.months_between(F.coalesce("deathdate", F.current_date()), "birthdate") / 12)
    dim_patient = pat.select(
        "patient_id", "gender", "race", "ethnicity", "state", "county", "birthdate",
        F.when(age < 18, "0-17").when(age < 40, "18-39").when(age < 65, "40-64").otherwise("65+").alias("age_group"),
        c("deathdate").isNotNull().alias("is_deceased"),
    )

    dates = (
        enc.select(F.to_date("start_ts").alias("d"))
        .union(med.select(F.to_date("start_ts")))
        .union(cond.select("start_date"))
        .distinct()
        .where(c("d").isNotNull())
    )
    dim_date = dates.select(
        date_key("d").alias("date_key"), c("d").alias("date"), F.year("d").alias("year"),
        F.quarter("d").alias("quarter"), F.month("d").alias("month"), F.dayofweek("d").alias("day_of_week"),
    )

    dim_condition = cond.groupBy("code").agg(F.first("description").alias("description"))

    fact_encounter = enc.select(
        "encounter_id", "patient_id", date_key("start_ts").alias("date_key"), "start_ts", "stop_ts",
        "encounter_class", "reason_code", "reason_description", "total_claim_cost", "payer_coverage",
        ((F.unix_timestamp("stop_ts") - F.unix_timestamp("start_ts")) / 60).alias("duration_min"),
        "year",
    )

    fact_medication = med.select(
        "patient_id", "encounter_id", date_key("start_ts").alias("date_key"), "code", "description",
        "dispenses", "total_cost", "year",
    )

    fact_condition = cond.select(
        "patient_id", "encounter_id", date_key("start_date").alias("date_key"), "code",
        "start_date", "stop_date",
    )

    return {
        "dim_patient": dim_patient,
        "dim_date": dim_date,
        "dim_condition": dim_condition,
        "fact_encounter": fact_encounter,
        "fact_medication": fact_medication,
        "fact_condition": fact_condition,
    }


def run(spark):
    silver = {t: spark.read.parquet(str(SILVER / t)) for t in ("patients", "encounters", "conditions", "medications")}
    gold = build(silver)
    for name, df in gold.items():
        writer = df.write.mode("overwrite")
        if "year" in df.columns:
            writer = writer.partitionBy("year")
        writer.parquet(str(GOLD / name))
        print(f"gold.{name}: {spark.read.parquet(str(GOLD / name)).count()} rows")
    return gold
