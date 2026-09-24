from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ArrayType,
)


def schema_resenas():
    """Contrato raw de reseñas, preservando tags y respuestas como estructuras anidadas."""
    respuesta = StructType([
        StructField("autor", StringType(), True),
        StructField("texto", StringType(), True),
    ])

    return StructType([
        StructField("resena_id", StringType(), True),
        StructField("producto_id", StringType(), True),
        StructField("cliente_id", StringType(), True),
        StructField("calificacion", LongType(), True),
        StructField("comentario", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("respuestas", ArrayType(respuesta), True),
        StructField("fecha_resena", StringType(), True),
    ])
