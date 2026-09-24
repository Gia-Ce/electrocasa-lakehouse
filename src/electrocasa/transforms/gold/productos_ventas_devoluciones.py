from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, countDistinct, coalesce, lit, row_number, desc
from pyspark.sql.window import Window


def transform_productos_ventas_devoluciones(
    ventas: DataFrame,
    devoluciones: DataFrame,
    productos: DataFrame,
) -> DataFrame:
    ventas_agg = ventas.groupBy("producto_id").agg(
        sum("cantidad").alias("unidades_vendidas"),
        countDistinct("venta_id").alias("cantidad_ventas"),
    )

    devoluciones_agg = devoluciones.groupBy("producto_id").agg(
        countDistinct("devolucion_id").alias("cantidad_devoluciones"),
        sum("monto_reembolso").alias("monto_reembolsado"),
    )

    # Se omite precio_lista para no generar fanout cuando un producto tiene snapshots con distinto precio.
    productos_ref = productos.select("producto_id", "nombre_producto", "categoria").distinct()

    return (
        ventas_agg
        .join(devoluciones_agg, "producto_id", "full")
        .join(productos_ref, "producto_id", "left")
        .withColumn("unidades_vendidas", coalesce("unidades_vendidas", lit(0)))
        .withColumn("cantidad_ventas", coalesce("cantidad_ventas", lit(0)))
        .withColumn("cantidad_devoluciones", coalesce("cantidad_devoluciones", lit(0)))
        .withColumn("monto_reembolsado", coalesce("monto_reembolsado", lit(0.0)))
        .withColumn("ranking_ventas", row_number().over(Window.orderBy(desc("unidades_vendidas"))))
        .withColumn("ranking_devoluciones", row_number().over(Window.orderBy(desc("cantidad_devoluciones"))))
    )
