USE CATALOG electrocasa;

-- 1. Inventario
SELECT table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold', 'observability')
ORDER BY table_schema, table_name;

-- 2. Reglas críticas de Silver. Esperado: infracciones = 0 y filas > 0.
SELECT 'ventas' AS tabla, COUNT(*) AS filas,
       SUM(CASE WHEN monto_total IS NULL OR monto_total <= 0 THEN 1 ELSE 0 END) AS infracciones
FROM silver.ventas
UNION ALL
SELECT 'resenas', COUNT(*),
       SUM(CASE WHEN calificacion IS NULL OR calificacion NOT BETWEEN 1 AND 5 THEN 1 ELSE 0 END)
FROM silver.resenas
UNION ALL
SELECT 'devoluciones', COUNT(*),
       SUM(CASE WHEN monto_reembolso IS NULL OR monto_reembolso < 0 THEN 1 ELSE 0 END)
FROM silver.devoluciones;

-- 3. Motivos de cuarentena y trazabilidad
WITH rechazos AS (
  SELECT 'ventas' AS fuente, motivo_rechazo, fec_rechazo, id_lote FROM silver.ventas_cuarentena
  UNION ALL
  SELECT 'resenas', motivo_rechazo, fec_rechazo, id_lote FROM silver.resenas_cuarentena
  UNION ALL
  SELECT 'devoluciones', motivo_rechazo, fec_rechazo, id_lote FROM silver.devoluciones_cuarentena
  UNION ALL
  SELECT 'empleados', motivo_rechazo, fec_rechazo, id_lote FROM silver.empleados_cuarentena
)
SELECT fuente, motivo_rechazo, COUNT(*) AS registros,
       SUM(CASE WHEN id_lote IS NULL OR TRIM(id_lote) = '' OR fec_rechazo IS NULL
                THEN 1 ELSE 0 END) AS sin_trazabilidad
FROM rechazos
GROUP BY fuente, motivo_rechazo
ORDER BY fuente, registros DESC;

-- 4. Ambigüedad del catálogo antes de los joins Gold
WITH atributos AS (
  SELECT DISTINCT producto_id, nombre_producto, categoria FROM silver.productos
)
SELECT producto_id, COUNT(*) AS variantes_atributos
FROM atributos
GROUP BY producto_id
HAVING COUNT(*) > 1 OR producto_id IS NULL;

-- 5. Reembolsos por producto, detecta duplicaciones de Gold y productos faltantes en cualquiera de los lados

WITH esperado AS (
  SELECT producto_id, SUM(monto_reembolso) AS monto, 1 AS presente
  FROM silver.devoluciones GROUP BY producto_id
), observado AS (
  SELECT producto_id, SUM(monto_reembolsado) AS monto, COUNT(*) AS filas, 1 AS presente
  FROM gold.productos_ventas_devoluciones GROUP BY producto_id
)
SELECT COALESCE(e.producto_id, o.producto_id) AS producto_id,
       e.monto AS reembolso_silver, o.monto AS reembolso_gold, o.filas AS filas_gold
FROM esperado e FULL OUTER JOIN observado o ON e.producto_id <=> o.producto_id
WHERE o.presente IS NULL OR o.filas <> 1
   OR (e.presente IS NOT NULL AND NOT (ABS(e.monto - o.monto) <= 0.01))
   OR (e.presente IS NOT NULL AND o.monto IS NULL)
   OR (e.presente IS NULL AND NOT (o.monto <=> 0));

-- 6. Unicidad de versiones SCD2 
SELECT id_empleado, COUNT(*) AS versiones_vigentes
FROM silver.empleados_historial
WHERE __END_AT IS NULL
GROUP BY id_empleado
HAVING COUNT(*) > 1 OR id_empleado IS NULL;

-- 7. Intervalos SCD2 invertidos o superpuestos
WITH intervalos AS (
  SELECT id_empleado, __START_AT, __END_AT,
         MAX(COALESCE(__END_AT, CAST('9999-12-31' AS DATE))) OVER (
           PARTITION BY id_empleado ORDER BY __START_AT
           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
         ) AS fin_previo
  FROM silver.empleados_historial
)
SELECT * FROM intervalos
WHERE __START_AT IS NULL
   OR (__END_AT IS NOT NULL AND __END_AT <= __START_AT)
   OR __START_AT < fin_previo;

-- 8. Dotación por sucursal conciliada
WITH esperado AS (
  SELECT COALESCE(NULLIF(TRIM(sucursal_id), ''), 'sin_sucursal') AS sucursal_id,
         COUNT(DISTINCT id_empleado) AS empleados, 1 AS presente
  FROM silver.empleados_historial WHERE __END_AT IS NULL
  GROUP BY COALESCE(NULLIF(TRIM(sucursal_id), ''), 'sin_sucursal')
), observado AS (
  SELECT sucursal_id, SUM(empleados_activos) AS empleados, COUNT(*) AS filas, 1 AS presente
  FROM gold.dotacion_sucursal GROUP BY sucursal_id
)
SELECT COALESCE(e.sucursal_id, o.sucursal_id) AS sucursal_id,
       e.empleados AS esperado, o.empleados AS observado, o.filas
FROM esperado e FULL OUTER JOIN observado o ON e.sucursal_id <=> o.sucursal_id
WHERE e.presente IS NULL OR o.presente IS NULL OR o.filas <> 1
   OR NOT (e.empleados <=> o.empleados);

-- 9. Coherencia de tasas de reseñas
SELECT categoria, cantidad_resenas, resenas_negativas, tasa_resenas_negativas
FROM gold.resenas_categoria
WHERE cantidad_resenas IS NULL OR cantidad_resenas <= 0
   OR resenas_negativas IS NULL OR resenas_negativas < 0
   OR resenas_negativas > cantidad_resenas
   OR NOT (tasa_resenas_negativas <=>
           ROUND(100.0 * resenas_negativas / NULLIF(cantidad_resenas, 0), 2));

-- 10. Estado de las funciones de masking
SELECT current_user() AS usuario,
       is_account_group_member('electrocasa_engineers') AS es_engineer,
       silver.mask_dni('87654321') AS dni_prueba,
       silver.mask_salario(CAST(2750 AS DOUBLE)) AS salario_prueba;

-- 11. Inspeccionar en la salida las máscaras aplicadas a dni y salario
DESCRIBE TABLE EXTENDED silver.empleados_historial;

-- 12. Permisos configurados
SHOW GRANTS ON SCHEMA silver;
SHOW GRANTS ON SCHEMA gold;

-- 13. Errores recientes en los 3 pipelines
WITH eventos AS (
  SELECT 'bronze' AS capa, timestamp, level, event_type, message
  FROM observability.event_log_etl_electrocasa_ingest_bronze
  UNION ALL
  SELECT 'silver', timestamp, level, event_type, message
  FROM observability.event_log_etl_electrocasa_bronze_silver
  UNION ALL
  SELECT 'gold', timestamp, level, event_type, message
  FROM observability.event_log_etl_electrocasa_silver_gold
)
SELECT * FROM eventos
WHERE timestamp >= current_timestamp() - INTERVAL 48 HOURS
  AND level IN ('ERROR', 'WARN')
ORDER BY timestamp DESC
LIMIT 100;
