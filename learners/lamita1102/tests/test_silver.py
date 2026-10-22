import datetime as dt

from jobs.silver import split


COLS = (
    "code string, product_name string, brands string, "
    "categories string, countries string, nutrition_grade_fr string, "
    "sugars_100g string, last_modified string, "
    "loaded_at timestamp, source_file string"
)

T0 = dt.datetime(2026, 10, 13, 14, 0)


def bronze(spark, *rows):
    return spark.createDataFrame(
        [
            (code, name, "B", "Snacks", "France", "a",
             sugars, modified, T0, "test.csv")
            for code, name, sugars, modified in rows
        ],
        COLS,
    )


def test_each_rejected_row_names_its_rule(spark):
    raw = bronze(
        spark,
        ("001", "ok", "45", "2026-03-01"),
        (None, "nocode", "1", "2026-03-01"),
        ("003", "lots", "lots", "2026-03-01"),
        ("004", "badDate", "1", "March"),
        ("005", "nosugar", None, "2026-03-01"),
    )

    good, rejected = split(raw)

    assert sorted(
        (r.product_name, r.rule) for r in rejected.collect()
    ) == [
        ("badDate", "last_modified_unparseable"),
        ("lots", "sugars_not_a_number"),
        ("nocode", "code_missing"),
    ]

    assert sorted(
        (r.code, r.sugars_100g) for r in good.collect()
    ) == [
        ("001", 45.0),
        ("005", None),
    ]

    assert dict(good.dtypes)["sugars_100g"] == "double"
    assert dict(good.dtypes)["last_modified"] == "date"

    assert set(good.columns) == {
        "code", "product_name", "brands", "categories",
        "countries", "nutrition_grade_fr", "sugars_100g",
        "last_modified", "loaded_at",
    }

    assert good.count() + rejected.count() == raw.count()

    assert set(rejected.columns) == set(raw.columns) | {
        "rule", "rejected_at",
    }
    assert dict(rejected.dtypes)["sugars_100g"] == "string"
    assert rejected.filter("rejected_at IS NULL").count() == 0

    bad_sugar = rejected.filter("product_name = 'lots'").first()
    assert bad_sugar.sugars_100g == "lots"
    assert bad_sugar.source_file == "test.csv"
    assert bad_sugar.loaded_at == T0
