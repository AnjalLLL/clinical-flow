from conftest import ENCOUNTER_COLS, encounter, raw

from clinicalflow.quality.checks import failures, gold_checks, reconciles, silver_checks
from clinicalflow.transforms.bronze_to_silver import clean
from clinicalflow.transforms.silver_to_gold import build


def silver(raw_tables):
    return {t: clean(df, t)[0] for t, df in raw_tables.items()}


def test_clean_data_passes_every_check(raw_tables):
    s = silver(raw_tables)
    report = {"silver": silver_checks(s), "gold": gold_checks(build(s))}
    assert failures(report) == []


def test_orphan_encounter_is_caught(spark, raw_tables):
    raw_tables["encounters"] = raw(spark, ENCOUNTER_COLS, [encounter("e9", patient="ghost")])
    report = {"silver": silver_checks(silver(raw_tables))}
    # e1 is gone, so its medication is orphaned too: both breaks are reported
    assert failures(report) == [
        "silver.encounters: references:encounters.patient_id->patients (1 rows)",
        "silver.medications: references:medications.encounter_id->encounters (1 rows)",
    ]


def test_reconciliation_detects_lost_rows():
    assert reconciles({"bronze": 10, "silver": 7, "quarantined": 2, "duplicates": 1})["passed"]
    assert not reconciles({"bronze": 10, "silver": 6, "quarantined": 2, "duplicates": 1})["passed"]
