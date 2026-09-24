from pyspark import pipelines as dp
from src.electrocasa.transforms.gold.dotacion_sucursal import transform_dotacion_sucursal


@dp.materialized_view(name="dotacion_sucursal", comment="Dotacion activa de empleados por sucursal", table_properties={"quality": "gold"})
def dotacion_sucursal():
    return spark.read.table("silver.empleados_historial").transform(transform_dotacion_sucursal)
