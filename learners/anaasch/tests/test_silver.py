import datetime as dt
from jobs.silver import split, merge_silver

COLS = "code string, product_name string, brands string, categories string, countries string, " \
       "nutrition_grade_fr string, sugars_100g string, last_modified string, loaded_at timestamp"
T0 = dt.datetime(2026, 10, 13, 14, 0)


def bronze(spark, *rows):
    return spark.createDataFrame([(c, n, "B", "Snacks", "France", "a", s, m, T0) for c, n, s, m in rows], COLS)


def test_each_rejected_row_lists_all_its_rules(spark):
    good, rejected = split(bronze(spark,
        ("001", "ok",      "45",   "2026-03-01"),
        (None,  "nocode",  "1",    "2026-03-01"),
        ("003", "lots",    "lots", "2026-03-01"),
        ("004", "badDate", "1",    "March"),
        ("005", "nosugar", None,   "2026-03-01"),
        (None,  "both",    "lots", "2026-03-01")))
    assert sorted((r.product_name, r.rules) for r in rejected.collect()) == [
        ("badDate", ["last_modified_unparseable"]),
        ("both", ["code_missing", "sugars_not_a_number"]),
        ("lots", ["sugars_not_a_number"]),
        ("nocode", ["code_missing"])]
    assert sorted((r.code, r.sugars_100g) for r in good.collect()) == [("001", 45.0), ("005", None)]


def silver_rows(spark, path):
    return sorted((r.code, r.product_name, r.last_modified)
                  for r in spark.read.format("delta").load(path).collect())


def test_latest_last_modified_wins_and_rerun_changes_nothing(spark, tmp_path):
    path = str(tmp_path / "silver")
    good, _ = split(bronze(spark,
        ("001", "Choco",     "45", "2026-01-01"),
        ("001", "Choco bar", "44", "2026-02-01"),   # newer: wins
        ("001", "Choco bar", "44", "2026-02-01"),   # exact duplicate
        ("002", "Water",     "0",  "2026-01-01")))
    merge_silver(spark, good, path)
    late, _ = split(bronze(spark, ("002", "Old water", "0", "2025-12-01")))   # late update: older
    merge_silver(spark, late, path)
    want = [("001", "Choco bar", dt.date(2026, 2, 1)), ("002", "Water", dt.date(2026, 1, 1))]
    assert silver_rows(spark, path) == want
    merge_silver(spark, good, path)
    assert silver_rows(spark, path) == want