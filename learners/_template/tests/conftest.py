import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

def make_spark():
    builder = (SparkSession.builder.master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.ui.showConsoleProgress", "false"))
    return configure_spark_with_delta_pip(builder).getOrCreate()

@pytest.fixture(scope="session")
def spark():
    return make_spark()
