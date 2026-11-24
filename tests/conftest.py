import pytest

from clinicalflow.config import spark_session


@pytest.fixture(scope="session")
def spark():
    s = spark_session("clinicalflow-tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def raw(spark, header, rows):
    """Build a DataFrame shaped like a bronze CSV read: every column is a string."""
    return spark.createDataFrame(rows, ", ".join(f"{h} string" for h in header))


PATIENT_COLS = ["Id", "BIRTHDATE", "DEATHDATE", "GENDER", "RACE", "ETHNICITY", "STATE", "COUNTY", "INCOME"]
ENCOUNTER_COLS = ["Id", "START", "STOP", "PATIENT", "ENCOUNTERCLASS", "CODE", "DESCRIPTION",
                  "BASE_ENCOUNTER_COST", "TOTAL_CLAIM_COST", "PAYER_COVERAGE", "REASONCODE", "REASONDESCRIPTION"]
CONDITION_COLS = ["START", "STOP", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION"]
MEDICATION_COLS = ["START", "STOP", "PATIENT", "ENCOUNTER", "CODE", "DESCRIPTION", "BASE_COST", "DISPENSES", "TOTALCOST"]


def encounter(id_, patient="p1", start="2020-01-01T10:00:00Z", stop="2020-01-01T11:00:00Z",
              cls="inpatient", cost="100.0", reason="Pneumonia"):
    return (id_, start, stop, patient, cls, "c1", "Visit", "50.0", cost, "40.0", "r1", reason)


@pytest.fixture
def raw_tables(spark):
    return {
        "patients": raw(spark, PATIENT_COLS, [
            ("p1", "1980-05-01", None, "f", "White", "nonhispanic", "Massachusetts", "Essex County", "50000"),
            ("p2", "2015-02-10", None, "M", "asian", "nonhispanic", "Massachusetts", "Suffolk County", "30000"),
        ]),
        "encounters": raw(spark, ENCOUNTER_COLS, [
            encounter("e1"),
            encounter("e2", start="2020-01-20T09:00:00Z", stop="2020-01-22T09:00:00Z"),
            encounter("e3", patient="p2", cls="wellness", reason=None),
        ]),
        "conditions": raw(spark, CONDITION_COLS, [
            ("2020-01-01", None, "p1", "e1", "233604007", "Pneumonia"),
        ]),
        "medications": raw(spark, MEDICATION_COLS, [
            ("2020-01-01T10:00:00Z", None, "p1", "e1", "m1", "Amoxicillin", "10.0", "2", "20.0"),
        ]),
    }
