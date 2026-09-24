from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from src.common.utils import read_autoloader_stream, add_audit_columns
from src.electrocasa.schemas.bronze.resenas import schema_resenas

spark = SparkSession.builder.getOrCreate()

path_landing = spark.conf.get("path_landing")
path_schema_base = spark.conf.get("path_schema_base")


@dp.table(
    name="resenas",
    comment="Resenas raw cargadas desde JSON semiestructurado",
    table_properties={"quality": "bronze"},
)
def bronze_resenas():
    """Ingiere resenas incrementales conservando estructuras anidadas."""
    df = read_autoloader_stream(
        spark=spark,
        source_path=f"{path_landing}/resenas",
        schema_location=f"{path_schema_base}/resenas",
        schema=schema_resenas(),
        file_format="json",
        multi_line=True,
    )
    return add_audit_columns(df)
