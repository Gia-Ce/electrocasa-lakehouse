from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower, when


def transform_tracking(df: DataFrame) -> DataFrame:
    """Elimina reingestas exactas y homologa estados/couriers."""
    return (
        df
        .dropDuplicates([
            "tracking_id", "pedido_id", "courier", "estado_entrega",
            "sucursal_origen", "fecha_actualizacion",
        ])
        .withColumn("courier", lower(trim(col("courier"))))
        .withColumn(
            "estado_entrega",
            when(lower(trim(col("estado_entrega"))).isin("en camino", "en_camino", "en_transito"), "en_transito")
            .when(lower(trim(col("estado_entrega"))) == "entregado", "entregado")
            .when(lower(trim(col("estado_entrega"))) == "devuelto", "devuelto")
            .when(lower(trim(col("estado_entrega"))) == "pendiente", "pendiente")
            .otherwise(lower(trim(col("estado_entrega")))),
        )
    )
