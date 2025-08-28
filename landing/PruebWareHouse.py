import utils


# Configuracion Spark
# 1. Crear la sesión de Spark correctamente
spark = utils.create_context()
# 2. Obtener el SparkContext desde la SparkSession
sc = spark.sparkContext

#nombre de la base de datos y tabla raw
landing_db = "landing_db"
tbl_landing = "aemet_prediccion_diaria"
print(f" Leyendo tabla RAW: spark_catalog.{landing_db}.{tbl_landing}")
df_raw= spark.read.format("iceberg").load(f"spark_catalog.{landing_db}.{tbl_landing}")

#muesta cantidad de registros en raw
print(f"\n DataFrame RAW tiene {df_raw.count()} registros")

df_raw.show(20, truncate=False)

#cerrar spark
spark.stop()

