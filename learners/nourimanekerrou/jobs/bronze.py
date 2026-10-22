import sys
from pyspark.sql import SparkSession, functions as F


def load_bronze(spark, landing, bronze):
    # read every CSV under landing. 
    raw = spark.read.option("header", True).csv(landing)
    # stamp each row with its file and load time.
    stamped = raw.select(
        "*",
        F.col("_metadata.file_path").alias("source_file"),
        F.current_timestamp().alias("loaded_at"),
    )
    #append to the Delta table. 
    stamped.write.format("delta").mode("append").save(bronze)


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("bronze")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    load_bronze(spark, sys.argv[1], sys.argv[2])
    print("bronze rows:", spark.read.format("delta").load(sys.argv[2]).count())
