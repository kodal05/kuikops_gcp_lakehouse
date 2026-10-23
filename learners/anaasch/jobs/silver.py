import sys
from pyspark.sql import SparkSession, functions as F

def split(bronze_df):
    # Typed helper columns: unreadable values become null instead of failing.
    typed = bronze_df.withColumns({
        "_sugars": F.expr("try_cast(sugars_100g AS double)"),
        "_modified": F.to_date(F.try_to_timestamp("last_modified", F.lit("yyyy-MM-dd"))),
    })

    # First failing rule wins; null means the row passes all of them.
    r_code   = F.when(F.col("code").isNull() | (F.trim("code") == ""), "code_missing")
    r_date   = F.when(F.col("_modified").isNull(), "last_modified_unparseable")
    r_sugars = F.when(F.col("sugars_100g").isNotNull() & F.col("_sugars").isNull(), "sugars_not_a_number")
    judged = typed.withColumn("rules", F.array_compact(F.array(r_code, r_date, r_sugars)))

    # Quarantine keeps the original bronze row plus the rule.
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

import sys


if __name__ == "__main__":
    bronze, silver, quarantine = sys.argv[1], sys.argv[2], sys.argv[3]
    spark = (SparkSession.builder.appName("silver")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    good, rejected = split(spark.read.format("delta").load(bronze))
    good.write.format("delta").mode("overwrite").save(silver)
    rejected.write.format("delta").mode("overwrite").save(quarantine)
    rejected.select(F.explode("rules").alias("rule")).groupBy("rule").count().show(truncate=False)