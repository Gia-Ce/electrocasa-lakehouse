from pyspark import pipelines as dp
from src.electrocasa.transforms.gold.ventas_sucursal_mes import transform_ventas_sucursal_mes


@dp.materialized_view(
    name="ventas_sucursal_mes",
    comment="Ventas y ticket promedio por sucursal y mes",
    table_properties={"quality": "gold"},
)
def ventas_sucursal_mes():
    return spark.read.table("silver.ventas").transform(transform_ventas_sucursal_mes)
