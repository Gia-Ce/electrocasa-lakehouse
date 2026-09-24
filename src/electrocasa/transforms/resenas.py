from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when, lit, array, transform, struct, to_date


def transform_resenas(df: DataFrame, productos: DataFrame) -> DataFrame:
    """Normaliza estructuras anidadas y marca reseñas asociadas a productos huérfanos."""
    productos_ref = productos.select("producto_id").distinct().withColumn("producto_existe", lit(True))

    return (
        df
        .dropDuplicates()
        .withColumn("calificacion", col("calificacion").cast("int"))
        .withColumn("fecha_resena", to_date(col("fecha_resena"), "yyyy-MM-dd"))
        .withColumn(
            "comentario",
            when(col("comentario").isNull() | (trim(col("comentario")) == ""), lit(None))
            .otherwise(trim(col("comentario"))),
        )
        .withColumn("tags", when(col("tags").isNull(), array().cast("array<string>")).otherwise(col("tags")))
        .withColumn(
            "respuestas",
            when(
                col("respuestas").isNull(),
                array().cast("array<struct<autor:string,texto:string>>"),
            ).otherwise(
                transform(
                    col("respuestas"),
                    lambda x: struct(
                        when(x["autor"].isNull() | (trim(x["autor"]) == ""), lit("sin_autor"))
                        .otherwise(trim(x["autor"]))
                        .alias("autor"),
                        trim(x["texto"]).alias("texto"),
                    ),
                )
            ),
        )
        .join(productos_ref, "producto_id", "left")
        .withColumn("producto_huerfano", col("producto_existe").isNull())
        .drop("producto_existe")
    )
