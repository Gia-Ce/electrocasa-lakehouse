from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower, when, coalesce, to_date


def transform_ventas(df: DataFrame) -> DataFrame:
    """Deduplica, tipa y homologa ventas sin aplicar todavía la regla crítica de monto."""

    return (
        df
        .dropDuplicates([
            "venta_id", "sucursal_id", "producto_id", "cantidad",
            "monto_total", "metodo_pago", "fecha_venta", "canal",
        ])
        .withColumn("cantidad", col("cantidad").cast("int"))
        .withColumn("monto_total", col("monto_total").cast("double"))
        .withColumn(
            "fecha_venta",
            coalesce(
                to_date(col("fecha_venta"), "yyyy-MM-dd"),
                to_date(col("fecha_venta"), "dd/MM/yyyy"),
            ),
        )
        .withColumn(
            "metodo_pago",
            when(lower(trim(col("metodo_pago"))).isin("efectivo", "efv"), "efectivo")
            .when(
                lower(trim(col("metodo_pago"))).isin(
                    "tarjeta de credito", "tarjeta_credito", "tarjeta", "tc"
                ),
                "tarjeta_credito",
            )
            .when(
                lower(trim(col("metodo_pago"))).isin(
                    "transferencia", "transferencia bancaria"
                ),
                "transferencia",
            )
            .when(lower(trim(col("metodo_pago"))) == "plin", "plin")
            .when(lower(trim(col("metodo_pago"))) == "yape", "yape")
            .otherwise(lower(trim(col("metodo_pago"))))
        )
    )
