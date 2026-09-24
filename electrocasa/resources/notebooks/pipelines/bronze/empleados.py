from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from src.common.utils import read_autoloader_stream, add_audit_columns
from src.electrocasa.schemas.empleados import schema_empleados

spark = SparkSession.builder.getOrCreate()

path_landing = spark.conf.get("path_landing")
path_schema_base = spark.conf.get("path_schema_base")


@dp.table(
    name="empleados",
    comment="Eventos raw de empleados cargados desde archivos CSV",
    table_properties={"quality": "bronze"},
)
def bronze_empleados():
    """Ingiere eventos de RR.HH. por lote con Auto Loader."""
    df = read_autoloader_stream(
        spark=spark,
        source_path=f"{path_landing}/empleados",
        schema_location=f"{path_schema_base}/empleados",
        schema=schema_empleados(),
        file_format="csv",
        header=True,
    )
    return add_audit_columns(df)
