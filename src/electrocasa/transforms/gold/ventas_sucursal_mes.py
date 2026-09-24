from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when, date_trunc, sum, avg, countDistinct


def transform_ventas_sucursal_mes(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "sucursal_id",
            when(col("sucursal_id").isNull() | (trim(col("sucursal_id")) == ""), "sin_sucursal")
            .otherwise(trim(col("sucursal_id"))),
        )
        .withColumn("mes", date_trunc("month", col("fecha_venta")))
        .groupBy("sucursal_id", "mes")
        .agg(
            countDistinct("venta_id").alias("cantidad_ventas"),
            sum("monto_total").alias("monto_ventas"),
            avg("monto_total").alias("ticket_promedio"),
        )
    )
