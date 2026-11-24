from clinicalflow.transforms.bronze_to_silver import clean
from clinicalflow.transforms.silver_to_gold import build


def gold_from(raw_tables):
    return build({t: clean(df, t)[0] for t, df in raw_tables.items()})


def test_star_schema_tables_exist(raw_tables):
    assert set(gold_from(raw_tables)) == {
        "dim_patient", "dim_date", "dim_condition", "fact_encounter", "fact_medication", "fact_condition"}


def test_dim_patient_age_group(raw_tables):
    groups = {r.patient_id: r.age_group for r in gold_from(raw_tables)["dim_patient"].collect()}
    assert groups["p1"] in ("40-64",)
    assert groups["p2"] == "0-17"


def test_fact_encounter_duration_and_date_key(raw_tables):
    rows = {r.encounter_id: r for r in gold_from(raw_tables)["fact_encounter"].collect()}
    assert rows["e1"].duration_min == 60
    assert rows["e1"].date_key == 20200101
    assert rows["e2"].duration_min == 2 * 24 * 60


def test_every_fact_date_is_in_dim_date(raw_tables):
    g = gold_from(raw_tables)
    keys = {r.date_key for r in g["dim_date"].collect()}
    for fact in ("fact_encounter", "fact_medication", "fact_condition"):
        assert {r.date_key for r in g[fact].collect()} <= keys
