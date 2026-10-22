from jobs.bronze import load_bronze


def test_every_landed_row_is_stamped(spark, tmp_path):
    landing = tmp_path / "landing"; landing.mkdir()
    (landing / "a.csv").write_text("code,sugars_100g\n001,45\n002,0\n")
    (landing / "b.csv").write_text("code,sugars_100g\n001,44\n")

    load_bronze(spark, str(landing), str(tmp_path / "bronze"))

    b = spark.read.format("delta").load(str(tmp_path / "bronze"))
    got = sorted((r.code, r.sugars_100g, r.source_file.split("/")[-1])
                 for r in b.select("code", "sugars_100g", "source_file").collect())
    assert got == [("001", "44", "b.csv"), ("001", "45", "a.csv"), ("002", "0", "a.csv")]
    assert b.filter("loaded_at IS NULL").count() == 0
    