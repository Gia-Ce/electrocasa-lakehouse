from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, when, current_timestamp
from src.electrocasa.transforms.silver.empleados import transform_empleados


@dp.table(name="empleados_eventos", comment="Eventos de empleados con secuencia temporal confiable", table_properties={"quality": "silver"})
def empleados_eventos():
    return (
        spark.read.table("bronze.empleados")
        .transform(transform_empleados)
        .filter(col("fecha_evento").isNotNull() & (col("eventos_misma_fecha") == 1))
        .drop("eventos_misma_fecha")
    )


@dp.table(name="empleados_cuarentena", comment="Eventos de empleados no historizables de forma confiable", table_properties={"quality": "silver"})
def empleados_cuarentena():
    return (
        spark.read.table("bronze.empleados")
        .transform(transform_empleados)
        .filter(col("fecha_evento").isNull() | (col("eventos_misma_fecha") > 1))
        .withColumn(
            "motivo_rechazo",
            when(col("fecha_evento").isNull(), lit("fecha_evento_faltante"))
            .otherwise(lit("fecha_evento_ambigua")),
        )
        .withColumn("fec_rechazo", current_timestamp())
        .drop("eventos_misma_fecha")
    )
