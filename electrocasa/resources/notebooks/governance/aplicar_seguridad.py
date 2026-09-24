catalogo = dbutils.widgets.get("catalogo")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalogo}.silver.mask_dni(valor STRING)
RETURN CASE
  WHEN is_account_group_member('electrocasa_engineers') THEN valor
  ELSE '********'
END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalogo}.silver.mask_salario(valor DOUBLE)
RETURN CASE
  WHEN is_account_group_member('electrocasa_engineers') THEN valor
  ELSE NULL
END
""")

spark.sql(f"""
ALTER TABLE {catalogo}.silver.empleados_historial
ALTER COLUMN dni
SET MASK {catalogo}.silver.mask_dni
""")

spark.sql(f"""
ALTER TABLE {catalogo}.silver.empleados_historial
ALTER COLUMN salario
SET MASK {catalogo}.silver.mask_salario
""")
