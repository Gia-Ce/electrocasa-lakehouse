from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, lit, countDistinct, sum, when, round


def transform_resenas_categoria(resenas: DataFrame, productos: DataFrame) -> DataFrame:
    productos_ref = productos.select("producto_id", "categoria").distinct()

    return (
        resenas
        .join(productos_ref, "producto_id", "left")
        .withColumn("categoria", coalesce(col("categoria"), lit("sin_categoria")))
        .groupBy("categoria")
        .agg(
            countDistinct("resena_id").alias("cantidad_resenas"),
            sum(when(col("calificacion") <= 2, 1).otherwise(0)).alias("resenas_negativas"),
        )
        .withColumn(
            "tasa_resenas_negativas",
            round(col("resenas_negativas") / col("cantidad_resenas") * 100, 2),
        )
    )
