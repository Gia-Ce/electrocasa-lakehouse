from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when, countDistinct


def transform_dotacion_sucursal(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(col("__END_AT").isNull())
        .withColumn(
            "sucursal_id",
            when(col("sucursal_id").isNull() | (trim(col("sucursal_id")) == ""), "sin_sucursal")
            .otherwise(trim(col("sucursal_id"))),
        )
        .groupBy("sucursal_id")
        .agg(countDistinct("id_empleado").alias("empleados_activos"))
    )
