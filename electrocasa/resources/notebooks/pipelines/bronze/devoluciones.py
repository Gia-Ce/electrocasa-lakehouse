from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from src.common.utils import read_autoloader_stream, add_audit_columns
from src.electrocasa.schemas.bronze.devoluciones import schema_devoluciones

spark = SparkSession.builder.getOrCreate()

path_landing = spark.conf.get("path_landing")
path_schema_base = spark.conf.get("path_schema_base")


@dp.table(
    name="devoluciones",
    comment="Devoluciones raw cargadas desde archivos CSV",
    table_properties={"quality": "bronze"},
)
def bronze_devoluciones():
    """Ingiere devoluciones incrementales con Auto Loader."""
    df = read_autoloader_stream(
        spark=spark,
        source_path=f"{path_landing}/devoluciones",
        schema_location=f"{path_schema_base}/devoluciones",
        schema=schema_devoluciones(),
        file_format="csv",
        header=True,
    )
    return add_audit_columns(df)
