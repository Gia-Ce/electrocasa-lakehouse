from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from src.common.utils import read_autoloader_stream, add_audit_columns
from src.electrocasa.schemas.bronze.ventas import schema_ventas

spark = SparkSession.builder.getOrCreate()

path_landing = spark.conf.get("path_landing")
path_schema_base = spark.conf.get("path_schema_base")


@dp.table(
    name="ventas",
    comment="Ventas raw cargadas desde archivos CSV",
    table_properties={"quality": "bronze"},
)
def bronze_ventas():
    """Ingiere ventas incrementales con Auto Loader.

    Se usa una definicion explicita porque Ventas tiene frecuencia diaria y un
    formato estable; asi la configuracion queda visible en el propio pipeline.
    """
    df = read_autoloader_stream(
        spark=spark,
        source_path=f"{path_landing}/ventas",
        schema_location=f"{path_schema_base}/ventas",
        schema=schema_ventas(),
        file_format="csv",
        header=True,
    )
    return add_audit_columns(df)
