
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_bronze(
    spark: SparkSession,
    landing_path: str,
    bronze_path: str,
) -> DataFrame:
    raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(landing_path)
    )

    stamped = raw.select(
        "*",
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("loaded_at"),
    )

    stamped.write.format("delta").mode("append").save(bronze_path)

    return spark.read.format("delta").load(bronze_path)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("bronze")
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

    load_bronze(spark, sys.argv[1], sys.argv[2])

    print(
        "bronze rows:",
        spark.read.format("delta").load(sys.argv[2]).count(),
    )

    spark.stop()
