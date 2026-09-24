from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower, when, regexp_replace


def transform_productos(df: DataFrame) -> DataFrame:
    """Limpia el catálogo preservando snapshots sin inventar una versión 'más reciente'."""

    return (
        df
        .withColumn(
            "precio_lista",
            regexp_replace(trim(col("precio_lista")), "^S/\\s*", "").cast("double"),
        )
        .withColumn(
            "categoria",
            when(lower(trim(col("categoria"))).isin("climatizacion", "climatización"), "climatizacion")
            .when(lower(trim(col("categoria"))) == "cocina", "cocina")
            .when(lower(trim(col("categoria"))).isin("electronica", "electrónica"), "electronica")
            .when(lower(trim(col("categoria"))) == "entretenimiento", "entretenimiento")
            .when(lower(trim(col("categoria"))).isin("linea blanca", "línea blanca", "linea_blanca"), "linea_blanca")
            .otherwise(lower(trim(col("categoria")))),
        )
        .fillna({"marca": "sin_marca"})
    )
