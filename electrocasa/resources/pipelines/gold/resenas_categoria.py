from pyspark import pipelines as dp
from src.electrocasa.transforms.gold.resenas_categoria import transform_resenas_categoria


@dp.materialized_view(name="resenas_categoria", comment="Tasa de resenas negativas por categoria", table_properties={"quality": "gold"})
def resenas_categoria():
    return transform_resenas_categoria(
        spark.read.table("silver.resenas"),
        spark.read.table("silver.productos"),
    )
