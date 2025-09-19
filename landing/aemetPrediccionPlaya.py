import utils as utils
import requests
import pandas as pd
import json
import os
import time
import datetime
import random
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from http.client import RemoteDisconnected
from requests.exceptions import ConnectionError

# --- Configuración General ---
AEMET_API_KEY = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJmcnZhcmdhcy44N0BnbWFpbC5jb20iLCJqdGkiOiI3MTJmNjFkYi1hMDg3LTRkM2QtODFlNS04ZjY4YjYwOWE2YTAiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc0OTIyOTY1OSwidXNlcklkIjoiNzEyZjYxZGItYTA4Ny00ZDNkLTgxZTUtOGY2OGI2MDlhNmEwIiwicm9sZSI6IiJ9.BbMqB0Jj2_z5wJw6luQhH7iMlJDMk2gfPEVOQ7Chc7E'
API_CALL_DELAY_SECONDS = 3  # segundos entre llamadas a la API para no saturarla
LOG_PLAYAS = "errores_playas.log"

# Lista fija de playas
playas = pd.DataFrame([
    ["0303102", "Llevant / Playa de Levante", "03", "Alacant/Alicante", "03031", "Benidorm", "38º 32' 12\"", "-00º 07' 05\""],
    ["0303104", "Ponent / Playa de Poniente", "03", "Alacant/Alicante", "03031", "Benidorm", "38º 32' 14\"", "-00º 08' 55\""],
    ["0704001", "Cala Major", "07", "Illes Balears", "07040", "Palma de Mallorca", "39º 33' 12\"", "2º 36' 27\""],
    ["0704007", "Playa de Palma", "07", "Illes Balears", "07040", "Palma de Mallorca", "39º 31' 25\"", "2º 44' 17\""],
    ["0801903", "Del Bogatell", "08", "Barcelona", "08019", "Barcelona", "41º 23' 48\"", "2º 12' 19\""],
    ["0801905", "La Barceloneta", "08", "Barcelona", "08019", "Barcelona", "41º 23' 08\"", "2º 12' 02\""],
    ["4625001", "Playa de Levante / Malvarrosa", "46", "València/Valencia", "46250", "Valencia", "39º 28' 20\"", "-00º 19' 19\""],
    ["4625004", "La Devesa", "46", "València/Valencia", "46250", "Valencia", "39º 20' 22\"", "-00º 18' 19\""]
], columns=[
    "id_playa", "nombre_playa", "id_provincia", "nombre_provincia",
    "id_municipio", "nombre_municipio", "latitud", "longitud"
])

# --- Función para registrar errores ---
def registrar_error_playa(code, playa, municipio, error):
    """Registra playas fallidas en un log"""
    with open(LOG_PLAYAS, "a", encoding="utf-8") as f:
        f.write(f"{code},{playa},{municipio},{error}\n")


# --- Función para descargar datos de una playa ---
def descargar_prediccion_playa(api_key: str, playa: dict,
                               max_intentos=3, delay_seconds=5, timeout_seconds=10):
    """
    Descarga la predicción AEMET para una playa.
    """
    playa_id = playa["id_playa"]
    playa_name = playa["nombre_playa"]
    municipio = playa["nombre_municipio"]
    provincia = playa["nombre_provincia"]
    url_inicial = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/playa/{playa_id}"
    headers = {"accept": "application/json", "api_key": api_key}

    success = False
    try:
        # --- Obtener URL de datos ---
        for intento in range(max_intentos):
            try:
                response = requests.get(url_inicial, headers=headers, timeout=timeout_seconds)
                if response.status_code == 200:
                    break
                elif response.status_code in [429, 500]:
                    espera = (2 ** intento) * delay_seconds
                    print(f"⚠️ Error HTTP {response.status_code} en playa {playa_name} de {municipio}. Esperando {espera}s...")
                    time.sleep(espera)
                else:
                    print(f"❌ Error HTTP {response.status_code} obteniendo URL para {playa_name} de {municipio}")
                    return None
            except (RemoteDisconnected, ConnectionError, requests.Timeout) as e:
                espera = (2 ** intento) * 2
                print(f"⚠️ {type(e).__name__} en {playa_name} de {municipio}. Reintentando en {espera}s...")
                time.sleep(espera)
        else:
            print(f"❌ No se pudo obtener URL para {playa_name} de {municipio} tras {max_intentos} intentos.")
            registrar_error_playa(playa_id, playa_name, municipio,  "No se obtuvo URL tras intentos")
            return None

        data_info = response.json()
        if not data_info or 'datos' not in data_info:
            print(f"❌ Sin URL de datos en respuesta para {playa_name}")
            registrar_error_playa(playa_id, playa_name, municipio, "Sin URL de datos")
            return None

        data_url = data_info['datos']
        time.sleep(delay_seconds)

        # --- Descargar datos reales ---
        for intento in range(max_intentos):
            try:
                response_data = requests.get(data_url, timeout=timeout_seconds)
                if response_data.status_code == 200:
                    print(f"✅ Datos descargados para playa {playa_name} de municipio: {municipio}")
                    success = True
                    return {
                        "playa_codigo_aemet": playa_id,
                        "nombre_playa": playa_name,
                        "nombre_municipio": municipio,
                        "nombre_provincia": provincia,
                        "fecha_descarga_utc": datetime.datetime.utcnow().isoformat(),
                        "raw_aemet_data_json": response_data.json()
                    }
                elif response_data.status_code in [429, 500]:
                    espera = (2 ** intento) * delay_seconds
                    print(f"⚠️ Error {response_data.status_code} en descarga {playa_name} de {municipio}. Esperando {espera}s...")
                    time.sleep(espera)
                else:
                    print(f"❌ Error HTTP {response_data.status_code} descargando datos para {playa_name} de {municipio}")
                    break
            except (RemoteDisconnected, ConnectionError, requests.Timeout) as e:
                espera = (2 ** intento) * 2
                print(f"⚠️ {type(e).__name__} en descarga de {playa_name} de {municipio}. Reintentando en {espera}s...")
                time.sleep(espera)
        else:
            registrar_error_playa(playa_id, playa_name, municipio, "Falló descarga de datos")

    except Exception as e:
        print(f"❌ Error inesperado en {playa_name} de {municipio}: {str(e)}")
        registrar_error_playa(playa_id, playa_name, municipio, str(e))

    finally:
        time.sleep(delay_seconds)
        if not success:
            print(f"⚠️ Playa {playa_name} de {municipio} registrada en log de errores.")


# --- Función para reintentar errores ---
def reintentar_errores_playas(api_key):
    """
    Reintenta descargas de playas que fallaron y están en el log.
    Devuelve lista de diccionarios con los datos descargados.
    """
    
    if not os.path.exists(LOG_PLAYAS):
        print("✅ No hay errores pendientes en playas.")
        return []

    list_of_raw_jsons = []
    while os.path.exists(LOG_PLAYAS):
        print("\n🔄 Reintentando descargas fallidas desde el log de playas...")
        with open(LOG_PLAYAS, "r", encoding="utf-8") as f:
            errores = [line.strip().split(",") for line in f.readlines() if line.strip()]

        if not errores:
            print("✅ No quedan playas por reintentar. Descargas completas.")
            os.remove(LOG_PLAYAS)
            break

        print(f"\n 🔄 Reintentando {len(errores)} playas fallidas...")
        nuevos_errores = []
        for err in errores:
            if len(err) < 4:
                print(f"⚠️ Línea inválida en log: {err}")
                continue

            code, playa_name, municipio, provincia  = err[0], err[1], err[2], err[3]
            playa_dict = {
                "id_playa": code,
                "nombre_playa": playa_name,
                "nombre_municipio": municipio,
                "nombre_provincia": provincia
            }
            data = descargar_prediccion_playa(api_key, playa_dict)
            if data:
                list_of_raw_jsons.append(data)
            else:
                nuevos_errores.append((code, playa_name, municipio, provincia, "Reintento fallido"))

        # Reescribir log
        if nuevos_errores:
            with open(LOG_PLAYAS, "w", encoding="utf-8") as f:
                for code, playa_name, municipio, provincia, msg_error in nuevos_errores:
                    f.write(f"{code},{playa_name}, {municipio}, {provincia}, {msg_error}\n")
        else:
            if os.path.exists(LOG_PLAYAS):  
                os.remove(LOG_PLAYAS)

    return list_of_raw_jsons

data_type = "playa"
# --- MAIN ---
if __name__ == "__main__":
    print("\n🚀 Descargando datos de playas...")
    list_of_raw_jsons = []
   
    for _, row in playas.iterrows():
        nombre_playa = row["nombre_playa"]
        municipio = row["nombre_municipio"]
        provincia = row["nombre_provincia"]
        print(f"\nObteniendo datos para {nombre_playa} de {municipio}...")
        data = descargar_prediccion_playa(AEMET_API_KEY, row.to_dict())
        if data:
            list_of_raw_jsons.append(data)
        time.sleep(API_CALL_DELAY_SECONDS)

    print("\n🔁 Reintentando errores...")
    list_of_raw_jsons += reintentar_errores_playas(AEMET_API_KEY)

    print(f"\n✅ Descargas finalizadas. Total datos descargados: {len(list_of_raw_jsons)}")

# --- Crear DataFrame de Spark ---
    spark = utils.create_context()

    schema = StructType([
        StructField("playa_codigo_aemet", StringType(), True),
        StructField("nombre_municipio", StringType(), True),
        StructField("nombre_provincia", StringType(), True),
        StructField("fecha_descarga_utc", StringType(), True),
        StructField("raw_aemet_data_json_str", StringType(), True),
        ])
    
    df_raw_aemet = spark.createDataFrame(
        [
            (
                item["playa_codigo_aemet"],
                item["nombre_municipio"],
                item["nombre_provincia"],
                item["fecha_descarga_utc"],
                json.dumps(item["raw_aemet_data_json"], ensure_ascii=False),
            )
            for item in list_of_raw_jsons
        ],
        schema=schema
    )

    #Mostra mensaje de lista de datos guaradados con exito
    print(f"\n DataFrame RAW de Playas creado con {df_raw_aemet.count()} registros")
    df_raw_aemet.show(5, truncate=False)

    # --- Guardar en Iceberg ---
    landing_db = "landing_db"
    landing_table = f"aemet_prediccion_{data_type}"

    utils.append_iceberg_table(spark, df_raw_aemet, landing_db, landing_table)
    #mostrar datos guardados
    spark.stop()