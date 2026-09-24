catalogo = dbutils.widgets.get("catalogo")

# Ejemplo de monitoreo resumido del pipeline Silver.
display(spark.sql(f"""
SELECT level, event_type, COUNT(*) AS cantidad_eventos
FROM {catalogo}.observability.event_log_etl_electrocasa_bronze_silver
GROUP BY level, event_type
ORDER BY level, cantidad_eventos DESC
"""))
