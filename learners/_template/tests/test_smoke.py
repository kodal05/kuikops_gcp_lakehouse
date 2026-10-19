def test_delta_roundtrip(spark, tmp_path):
    path = str(tmp_path / "t")
    spark.createDataFrame([(1, "a")], ["n", "s"]).write.format("delta").save(path)
    assert spark.read.format("delta").load(path).count() == 1
