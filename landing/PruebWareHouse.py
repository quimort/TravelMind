import utils


# Configuracion Spark
# 1. Crear la sesión de Spark correctamente
spark = utils.create_context()
# 2. Obtener el SparkContext desde la SparkSession
sc = spark.sparkContext


# Ver el warehouse configurado
print("Warehouse configurado:", spark.conf.get("spark.sql.catalog.spark_catalog.warehouse"))

location = spark.sql("""
DESCRIBE FORMATTED local_db.aemetRawDiario
""").filter("col_name = 'Location'").first()[1]
print(location)