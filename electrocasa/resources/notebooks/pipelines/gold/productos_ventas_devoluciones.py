from pyspark import pipelines as dp
from src.electrocasa.transforms.gold.productos_ventas_devoluciones import transform_productos_ventas_devoluciones


@dp.materialized_view(name="productos_ventas_devoluciones", comment="Ranking de productos vendidos y devueltos", table_properties={"quality": "gold"})
def productos_ventas_devoluciones():
    return transform_productos_ventas_devoluciones(
        spark.read.table("silver.ventas"),
        spark.read.table("silver.devoluciones"),
        spark.read.table("silver.productos"),
    )
