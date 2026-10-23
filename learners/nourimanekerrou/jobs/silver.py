import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import Window
from delta import DeltaTable

STRING_COLS = ["code", "product_name", "brands", "categories", "countries", "nutrition_grade_fr"]
SILVER_COLUMNS = STRING_COLS + ["sugars_100g", "last_modified"]


def split(bronze_df):
    # 1. Typer, sans jamais planter : try_cast / try_to_timestamp donnent null si illisible.
    sugars = F.expr("try_cast(sugars_100g AS double)")
    modified = F.to_date(F.try_to_timestamp("last_modified", F.lit("yyyy-MM-dd")))
    typed = bronze_df.withColumns({"_sugars": sugars, "_modified": modified})

    # 2. Juger : la première règle ratée, ou null si aucune.
    rule = (F.when(F.col("code").isNull() | (F.trim("code") == ""), "code_missing")
             .when(F.col("_modified").isNull(), "last_modified_unparseable")
             .when(F.col("sugars_100g").isNotNull() & F.col("_sugars").isNull(), "sugars_not_a_number"))
    judged = typed.withColumn("rule", rule)

    # 3a. good : les lignes sans règle, avec les vrais types.
    good = judged.filter(F.col("rule").isNull()).select(
        *STRING_COLS,
        F.col("_sugars").alias("sugars_100g"),
        F.col("_modified").alias("last_modified"),
        "loaded_at",
    )

    # 3b. rejected : la ligne telle que bronze l'avait (que des strings), + rule + rejected_at.
    rejected = (judged.filter(F.col("rule").isNotNull())
                .drop("_sugars", "_modified")
                .withColumn("rejected_at", F.current_timestamp()))

    return good, rejected

def latest(good):
    # Une seule ligne par code : la plus récente, puis le chargement le plus récent en cas d'égalité.
    w = Window.partitionBy("code").orderBy(F.desc("last_modified"), F.desc("loaded_at"))
    return good.withColumn("_n", F.row_number().over(w)).filter("_n = 1").select(*SILVER_COLUMNS)


def merge_silver(spark, good, silver):
    source = latest(good)
    if not DeltaTable.isDeltaTable(spark, silver):
        source.limit(0).write.format("delta").save(silver)   # 1er run : on crée la table vide
    (DeltaTable.forPath(spark, silver).alias("t")
        .merge(source.alias("s"), "t.code = s.code")
        .whenMatchedUpdateAll(condition="s.last_modified > t.last_modified")
        .whenNotMatchedInsertAll()
        .execute())


def run(spark, bronze, silver, quarantine):
    good, rejected = split(spark.read.format("delta").load(bronze))
    rejected.write.format("delta").mode("overwrite").save(quarantine)
    print("quarantine by rule:")
    rejected.groupBy("rule").count().show()
    merge_silver(spark, good, silver)
    print("silver rows:", spark.read.format("delta").load(silver).count())


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("silver")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    run(spark, *sys.argv[1:4])