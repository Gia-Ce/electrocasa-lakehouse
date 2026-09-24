from pyspark import pipelines as dp
from src.electrocasa.transforms.silver.tracking import transform_tracking


@dp.table(
    name="tracking_envios",
    comment="Tracking de envios limpio y estandarizado",
    table_properties={"quality": "silver"},
)
def silver_tracking():
    return spark.read.table("bronze.tracking_envios").transform(transform_tracking)
