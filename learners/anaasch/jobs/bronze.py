# learners/<handle>/jobs/bronze.py: the skeleton. You write the body.
import sys
from pyspark.sql import SparkSession, functions as F


def load_bronze(spark, landing, bronze):
    # Bronze = raw data, appended as-is. No cleaning, no typing:
    # every column stays a string, so nothing is lost at load time.
    # The only transformation is adding traceability columns.

    # Read every CSV in the landing folder. header=True takes the first
    # line as column names. Without inferSchema, all columns are strings.
    raw = spark.read.option("header", True).csv(landing)

    # Add two traceability columns:
    # - source_file: which landing file each row came from (Spark's _metadata)
    # - loaded_at: when this batch loaded the row
    stamped = raw.select(
        "*",
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("loaded_at"),
    )

    # Append to the Delta table in bronze. Append (not overwrite) keeps
    # previous drops, so each drop adds rows and creates a new commit
    # in _delta_log/.
    stamped.write.format("delta").mode("append").save(bronze)



if __name__ == "__main__":
    spark = (SparkSession.builder.appName("bronze")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    load_bronze(spark, sys.argv[1], sys.argv[2])
    print("bronze rows:", spark.read.format("delta").load(sys.argv[2]).count())