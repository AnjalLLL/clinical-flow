from conftest import ENCOUNTER_COLS, PATIENT_COLS, encounter, raw

from clinicalflow.transforms.bronze_to_silver import clean


def test_patients_are_typed_standardised_and_drop_identifiers(raw_tables):
    good, bad, stats = clean(raw_tables["patients"], "patients")
    row = good.filter("patient_id = 'p1'").first()
    assert str(row.birthdate) == "1980-05-01"
    assert row.gender == "F" and row.race == "white"
    assert row.income == 50000
    assert "SSN" not in good.columns and "FIRST" not in good.columns
    assert stats == {"bronze": 2, "quarantined": 0, "duplicates": 0, "silver": 2}


def test_bad_encounters_go_to_quarantine_with_reason(spark):
    df = raw(spark, ENCOUNTER_COLS, [
        encounter("ok"),
        encounter(None),
        encounter("neg", cost="-5"),
        encounter("backwards", start="2020-02-02T10:00:00Z", stop="2020-02-01T10:00:00Z"),
    ])
    good, bad, stats = clean(df, "encounters")
    reasons = {r.encounter_id: r.dq_reason for r in bad.collect()}
    assert reasons == {None: "null_encounter_id", "neg": "negative_cost", "backwards": "stop_before_start"}
    assert [r.encounter_id for r in good.collect()] == ["ok"]
    assert stats["quarantined"] == 3


def test_duplicates_are_removed_and_counted(spark):
    df = raw(spark, ENCOUNTER_COLS, [encounter("e1"), encounter("e1"), encounter("e2")])
    good, _, stats = clean(df, "encounters")
    assert good.count() == 2
    assert stats == {"bronze": 3, "quarantined": 0, "duplicates": 1, "silver": 2}


def test_missing_stop_date_is_not_an_error(spark):
    df = raw(spark, PATIENT_COLS, [("p1", "1980-05-01", None, "F", "white", "x", "MA", "c", "1")])
    _, bad, _ = clean(df, "patients")
    assert bad.count() == 0


def test_encounters_get_year_partition(raw_tables):
    good, _, _ = clean(raw_tables["encounters"], "encounters")
    assert {r.year for r in good.select("year").collect()} == {2020}
