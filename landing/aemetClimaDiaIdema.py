import utils as utils
import requests
import json
import sys
import os
from pyspark.sql import SparkSession

# =========================
# Configuración
# =========================
AEMET_API_KEY = os.getenv("AEMET_API_KEY")  # O coloca tu API key directamente
DB_NAME = "landing_db"
TABLE_NAME = "aemetClimaDiario"


# =========================
# Función para obtener datos de AEMET
# =========================
def get_aemet_climatologicos(api_key, fecha_ini, fecha_fin, estacion_id):
    url = f"https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{fecha_ini}/fechafin/{fecha_fin}/estacion/{estacion_id}"

    headers = {"api_key": api_key}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Error en API AEMET: {response.status_code}, {response.text}")

    data_json = response.json()

    if data_json["estado"] != 200:
        raise Exception(f"Error en respuesta AEMET: {data_json}")

    datos_url = data_json["datos"]

    # Descargar los datos reales
    datos_resp = requests.get(datos_url)
    datos_resp.raise_for_status()

    return datos_resp.json()


# =========================
# Guardar en Iceberg
# =========================
def save_to_iceberg(spark, data, db_name, table_name):
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))

    full_name = f"spark_catalog.{db_name}.{table_name}"

    if spark.catalog.tableExists(f"{db_name}.{table_name}"):
        print(f"⚡ Tabla {full_name} ya existe. Se hará append de los datos...")
        df.writeTo(full_name).using("iceberg").append()
    else:
        print(f"✨ Creando tabla {full_name}...")
        df.writeTo(full_name).using("iceberg").create()


# =========================
# Script principal
# =========================
if __name__ == "__main__":
    fecha_ini = "2020-01-01"
    fecha_fin = "2020-01-30"
    estacion = "0201D"

    print("🚀 Descargando datos climatológicos diarios de AEMET...")

    # Descargar datos
    data = get_aemet_climatologicos(AEMET_API_KEY, fecha_ini, fecha_fin, estacion)

    #ver el contenido de data
    print(data)

    #print(f"✅ Datos descargados. Total registros: {len(data)}")
    # Crear sesión Spark
    #spark = utils.create_context()

    # Guardar en Iceberg
    #save_to_iceberg(spark, data, DB_NAME, TABLE_NAME)

    #print("✅ Datos guardados correctamente en Iceberg.")
