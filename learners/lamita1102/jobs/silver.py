from pyspark.sql import functions as F


def split(bronze_df):
    typed = bronze_df.withColumns({
        "_sugars": F.expr("try_cast(sugars_100g AS double)"),
        "_modified": F.to_date(
            F.try_to_timestamp(
                "last_modified", F.lit("yyyy-MM-dd")
            )
        ),
    })

    rule = (
        F.when(
            F.col("code").isNull() | (F.trim("code") == ""),
            "code_missing",
        )
        .when(
            F.col("_modified").isNull(),
            "last_modified_unparseable",
        )
        .when(
            F.col("sugars_100g").isNotNull()
            & F.col("_sugars").isNull(),
            "sugars_not_a_number",
        )
    )

    judged = typed.withColumn("rule", rule)

    good = judged.filter(F.col("rule").isNull()).select(
        "code",
        "product_name",
        "brands",
        "categories",
        "countries",
        "nutrition_grade_fr",
        F.col("_sugars").alias("sugars_100g"),
        F.col("_modified").alias("last_modified"),
        "loaded_at",
    )

    rejected = (
        judged.filter(F.col("rule").isNotNull())
        .select(*bronze_df.columns, "rule")
        .withColumn("rejected_at", F.current_timestamp())
    )

    return good, rejected



def latest(good):
    from pyspark.sql import Window

    window = Window.partitionBy("code").orderBy(
        F.desc("last_modified"),
        F.desc("loaded_at"),
    )

    return (
        good.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def merge_silver(spark, good, silver):
    from delta import DeltaTable

    source = latest(good)

    if not DeltaTable.isDeltaTable(spark, silver):
        source.limit(0).write.format("delta").save(silver)

    (
        DeltaTable.forPath(spark, silver).alias("t")
        .merge(source.alias("s"), "t.code = s.code")
        .whenMatchedUpdateAll(
            condition="s.last_modified > t.last_modified"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )



def run(spark, bronze, silver, quarantine):
    bronze_df = spark.read.format("delta").load(bronze)
    good, rejected = split(bronze_df)

    rejected.write.format("delta").mode("overwrite").save(quarantine)
    rejected.groupBy("rule").count().show(truncate=False)

    merge_silver(spark, good, silver)

    print("silver rows:", spark.read.format("delta").load(silver).count())


if __name__ == "__main__":
    import sys
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("silver")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    run(spark, *sys.argv[1:4])
    spark.stop()
