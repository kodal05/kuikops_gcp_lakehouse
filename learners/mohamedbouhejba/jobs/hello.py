# jobs/hello.py: write three rows to Delta in your bronze bucket, read them back
import sys
from pyspark.sql import SparkSession

bronze = sys.argv[1]
spark = (SparkSession.builder.appName("hello")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())
df = spark.createDataFrame([(1, "landing"), (2, "bronze"), (3, "silver")], ["n", "layer"])
df.write.format("delta").mode("overwrite").save(f"{bronze}/hello")
print("rows:", spark.read.format("delta").load(f"{bronze}/hello").count())
