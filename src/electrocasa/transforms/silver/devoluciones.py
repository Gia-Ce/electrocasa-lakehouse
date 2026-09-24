from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when, lit, to_date


def transform_devoluciones(df: DataFrame, productos: DataFrame) -> DataFrame:
    """Limpia devoluciones y marca referencias a productos inexistentes."""
    productos_ref = productos.select("producto_id").distinct().withColumn("producto_existe", lit(True))

    return (
        df
        .dropDuplicates([
            "devolucion_id", "pedido_id", "sucursal_id", "producto_id",
            "motivo", "monto_reembolso", "fecha_devolucion",
        ])
        .withColumn("monto_reembolso", col("monto_reembolso").cast("double"))
        .withColumn("fecha_devolucion", to_date(col("fecha_devolucion"), "yyyy-MM-dd"))
        .withColumn(
            "motivo",
            when(col("motivo").isNull() | (trim(col("motivo")) == ""), lit(None))
            .otherwise(trim(col("motivo"))),
        )
        .withColumn("pedido_faltante", col("pedido_id").isNull() | (trim(col("pedido_id")) == ""))
        .join(productos_ref, "producto_id", "left")
        .withColumn("producto_huerfano", col("producto_existe").isNull())
        .drop("producto_existe")
    )
