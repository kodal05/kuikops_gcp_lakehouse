import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable


def split(bronze_df):
    # Typed helper columns: unreadable values become null instead of failing.
    typed = bronze_df.withColumns({
        "_sugars": F.expr("try_cast(sugars_100g AS double)"),
        "_modified": F.to_date(F.try_to_timestamp("last_modified", F.lit("yyyy-MM-dd"))),
    })

    # Every rule the row fails, as an array. Nulls (rules passed) are removed.
    r_code   = F.when(F.col("code").isNull() | (F.trim("code") == ""), "code_missing")
    r_date   = F.when(F.col("_modified").isNull(), "last_modified_unparseable")
    r_sugars = F.when(F.col("sugars_100g").isNotNull() & F.col("_sugars").isNull(), "sugars_not_a_number")
    judged = typed.withColumn("rules", F.array_compact(F.array(r_code, r_date, r_sugars)))

    # Quarantine keeps the original bronze row plus the rules.
    rejected = (judged.filter(F.size("rules") > 0)
                .withColumn("rejected_at", F.current_timestamp())
                .drop("_sugars", "_modified"))

    # Good rows: typed columns only, exactly the list the exercise asks for.
    good = (judged.filter(F.size("rules") == 0)
            .select(
                "code", "product_name", "brands", "categories", "countries",
                "nutrition_grade_fr",
                F.col("_sugars").alias("sugars_100g"),
                F.col("_modified").alias("last_modified"),
                "loaded_at",
            ))

    return good, rejected


SILVER_COLUMNS = ["code", "product_name", "brands", "categories", "countries",
                  "nutrition_grade_fr", "sugars_100g", "last_modified"]


def latest(good):
    # One row per code: the newest last_modified wins, then the latest load on a tie.
    w = Window.partitionBy("code").orderBy(F.desc("last_modified"), F.desc("loaded_at"))
    return (good.withColumn("_n", F.row_number().over(w))
                .filter("_n = 1")
                .select(*SILVER_COLUMNS))


def merge_silver(spark, good, silver):
    source = latest(good)
    # First run: the Delta table does not exist yet, so create it empty.
    if not DeltaTable.isDeltaTable(spark, silver):
        source.limit(0).write.format("delta").save(silver)
    (DeltaTable.forPath(spark, silver).alias("t")
        .merge(source.alias("s"), "t.code = s.code")
        .whenMatchedUpdateAll(condition="s.last_modified > t.last_modified")
        .whenNotMatchedInsertAll()
        .execute())


def run(spark, bronze, silver, quarantine):
    good, rejected = split(spark.read.format("delta").load(bronze))
    # Quarantine is recomputed from all of bronze each run, so overwrite (not append).
    rejected.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(quarantine)
    merge_silver(spark, good, silver)
    return good, rejected


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("silver")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    good, rejected = run(spark, *sys.argv[1:4])
    print("silver rows:", good.count(), "quarantined rows:", rejected.count())
    rejected.select(F.explode("rules").alias("rule")).groupBy("rule").count().show(truncate=False)