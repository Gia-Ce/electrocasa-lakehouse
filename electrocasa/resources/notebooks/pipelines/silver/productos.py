from pyspark import pipelines as dp
from src.electrocasa.transforms.silver.productos import transform_productos


@dp.table(
    name="productos",
    comment="Catalogo de productos limpio y estandarizado",
    table_properties={"quality": "silver"},
)
def silver_productos():
    return spark.read.table("bronze.productos").transform(transform_productos)
