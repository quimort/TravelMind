import utils
import json
from pyspark.sql.types import *
from pyspark.sql import functions as F

##Configuracion general
db_name = "landing_db"
table_name = "aemetClimaDiario"
particiones = 200  # Ajusta según RAM

# Configuracion Spark
# 1. Crear la sesión de Spark correctamente
spark = utils.create_context()
# 2. Obtener el SparkContext desde la SparkSession
#sc = spark.sparkContext
nombre_archivo = "aemet_unificado_2010-01-01_2024-12-31.json"

print("Warehouse configurado:", spark.conf.get("spark.sql.catalog.spark_catalog.warehouse"))
# 3. Definir el esquema para el DataFrame
esquema = StructType([
    StructField("fecha", StringType()),  # o StringType() si prefieres mantenerlo como texto
    StructField("indicativo", StringType()),
    StructField("nombre", StringType()),
    StructField("provincia", StringType()),
    StructField("altitud", StringType()),
    StructField("tmed", StringType()),      
    StructField("prec", StringType()),
    StructField("tmin", StringType()),
    StructField("horatmin", StringType()),
    StructField("tmax", StringType()),
    StructField("horatmax", StringType()),
    StructField("dir", StringType()),  # dirección del viento (grados)
    StructField("velmedia", StringType()),  # velocidad media del viento
    StructField("racha", StringType()),  # ráfaga máxima
    StructField("horaracha", StringType()),
    StructField("hrMedia", StringType()),  # humedad relativa media
    StructField("hrMax", StringType()),  # humedad relativa máxima
    StructField("horaHrMax", StringType()),
    StructField("hrMin", StringType()),  # humedad relativa mínima
    StructField("horaHrMin", StringType())
])
# 4. Cargar json y convertir a DataFrame
# --- Función para extraer registros de un año directamente desde JSON ---
def extraer_año(nombre_archivo, año):
    datos_año = []
    with open(nombre_archivo, "r", encoding="utf-8") as f:
        contenido = json.load(f)  # carga la estructura completa {"data": [...]}
        for registro in contenido["data"]:
            if registro["fecha"].startswith(str(año)):
                datos_año.append(registro)
    return datos_año

# --- Procesar año por año ---
años = list(range(2010, 2024))  # 2010 a 2024
for y in años:
    print(f"\nProcesando año: {y}")
    datos_año = extraer_año(nombre_archivo, y)
    
    if not datos_año:
        print(f"No hay datos para el año {y}")
        continue
    # Crear DataFrame Spark
    df_año = spark.createDataFrame(datos_año, schema=esquema)
    # Agregar columnas year y month
    df_año = df_año.withColumn("fecha_dt", F.to_date("fecha", "yyyy-MM-dd")) \
                   .withColumn("year", F.year("fecha_dt")) \
                   .withColumn("month", F.month("fecha_dt")) \
                   .repartition(particiones)
    print(f"Registros año {y}: {df_año.count()}")

    # Crear DataFrame Spark
    df_año = spark.createDataFrame(datos_año, schema=esquema)

    # Guardar en Iceberg
    utils.append_iceberg_table(spark, df_año, db_name, table_name)
    print(f"Año {y} agregado a Iceberg correctamente.")

# --- Verificar ubicación tabla ---
location = (
    spark.sql(f"DESCRIBE FORMATTED {db_name}.{table_name}")
         .filter("col_name = 'Location'")
         .select("data_type")
         .collect()[0][0]
)
print(f"\nTabla Iceberg guardada en: {location}")

spark.stop()
