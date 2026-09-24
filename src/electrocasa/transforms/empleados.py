from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, lower, regexp_replace, count
from pyspark.sql.window import Window


def transform_empleados(df: DataFrame) -> DataFrame:
    """Normaliza RR.HH. y detecta eventos sin secuencia temporal confiable.

    id_empleado se mantiene como business key porque DNI puede ser nulo,
    compartido o cambiar entre eventos.
    """
    ventana = Window.partitionBy("id_empleado", "fecha_evento")

    return (
        df
        .withColumn("nombre", trim(col("nombre")))
        .withColumn("dni", trim(col("dni")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("salario", col("salario").cast("double"))
        .withColumn("fecha_evento", col("fecha_evento").cast("date"))
        .withColumn("tipo_evento", lower(regexp_replace(trim(col("tipo_evento")), " ", "_")))
        .withColumn("salario_atipico", col("salario") > 10000)
        .withColumn("eventos_misma_fecha", count("*").over(ventana))
    )
