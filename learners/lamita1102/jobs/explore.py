import os
import tempfile

from pyspark.sql import functions as F
from tests.conftest import make_spark

spark = make_spark()

d = tempfile.mkdtemp()
landing = f"{d}/landing"
bronze = f"{d}/bronze"

os.makedirs(landing)

with open(f"{landing}/drop1.csv", "w") as fichier:
    fichier.write(
        "code,product_name,sugars_100g,last_modified\n"
        "001,Chocolate,30.0,2026-01-01\n"
        "002,Biscuits,20.0,2026-01-02\n"
        "003,Juice,10.0,2026-01-03\n"
    )

with open(f"{landing}/drop2.csv", "w") as fichier:
    fichier.write(
        "code,product_name,sugars_100g,last_modified\n"
        "001,Chocolate,30.0,2026-01-01\n"
        "004,Yogurt,5.0,2026-01-04\n"
    )

    # Lire les deux CSV
raw = spark.read.option("header", True).csv(landing)

# Ajouter la provenance et la date de chargement
stamped = raw.select(
    "*",
    F.col("_metadata.file_path").alias("source_file"),
    F.current_timestamp().alias("loaded_at")
)

# Ajouter les données dans la table Delta
stamped.write.format("delta").mode("append").save(bronze)

# Relire la table
b = spark.read.format("delta").load(bronze)

print("1. rows:", b.count())
print("2. distinct source files:", b.select("source_file").distinct().count())
print("3. sugars type:", dict(b.dtypes)["sugars_100g"])




try:
    stamped.withColumn("extra", F.lit(1)).write \
        .format("delta") \
        .mode("append") \
        .save(bronze)

    print("4. appended")
except Exception as e:
    print("4. refused:", str(e).splitlines()[0][:80])
    from delta import DeltaTable

print("5. Historique :")
DeltaTable.forPath(spark, bronze).history().select(
    "version", "operation"
).show()

print(
    "6. Lignes dans la version 0 :",
    spark.read.format("delta")
    .option("versionAsOf", 0)
    .load(bronze)
    .count()
)


b.select("code", "product_name", "sugars_100g").show()
spark.stop()

