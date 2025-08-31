import utils as utils

spark = utils.create_context()
#nombre de la base de datos y tabla raw
landing_db = "landing_db"
tbl_landing = "aemetClimaDiario"

# Ajusta estos nombres según tu configuración
trusted_db = "trusted_db"
tbl_trusted = "aemetClimaDiario_trusted"
print(f" Leyendo tabla RAW: spark_catalog.{landing_db}.{tbl_landing}")
#Leemos los datos de la tabla iceberg
df_raw= utils.read_iceberg_table(spark,db_name=landing_db,table_name=tbl_landing)