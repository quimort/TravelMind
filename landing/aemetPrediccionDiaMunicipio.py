import utils as utils
import requests
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
LOG_MUNCIPIOS = "errores_municipios.log"

# Lista fija de municipios que quieres consultar
MUNICIPIOS_FIJOS = [
    ("28079", "Madrid"),
    ("08019", "Barcelona"),
    ("41091", "Sevilla"),
    ("07040", "Palma de Mallorca"),
    ("03031", "Benidorm"),
    ("46250", "Valencia")
]

# --- Función para obtener datos de AEMET ---
def descargar_prediccion_municipio(api_key: str, municipio_code: str, municipio_name, data_type: str ,
                                    max_intentos=3, delay_seconds=5, timeout_seconds=10 ):
    """
    Descarga la predicción AEMET para un municipio.
    data_type puede ser 'diaria' o 'horaria'.
    """
    #configutacion URL
    url_inicial = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/{data_type}/{municipio_code}"
    headers = {"accept": "application/json", "api_key": api_key}

    # Validar tipo
    if data_type not in ["diaria", "horaria"]:
        print(f"Advertencia: Tipo de dato '{data_type}' no soportado. Usando 'diaria'.")
        data_type = 'diaria'
    
    success = False
    try:
        # --- Obtener URL de descarga ---
        for intento in range(max_intentos):
            try:
                response = requests.get(url_inicial, headers=headers, timeout=timeout_seconds)
                if response.status_code == 200:
                    break
                elif response.status_code in [429, 500]:
                    espera = (2 ** intento) * delay_seconds
                    print(f"⚠️ Error HTTP {response.status_code} en municipio {municipio_name}. Esperando {espera}s antes de reintentar nuevamente...")
                    time.sleep(espera)
                else:   
                    print(f"Error HTTP {response.status_code} en la solicitud URL de los datos")
                    data_url = None
                    break
            except (RemoteDisconnected, ConnectionError, requests.Timeout) as e:
                espera = (2 ** intento) * 2
                print(f"⚠️ {type(e).__name__} durante la descarga. Reintentando en {espera}s...")
                time.sleep(espera)
        else:
            # Esto se ejecuta si nunca se hizo break (fallaron todos los intentos)
            print(f"❌ No se pudo obtener la URL después de {max_intentos} intentos")
            registrar_error_municipio(code=municipio_code,
                                       name=municipio_name,
                                       error= f"No se obtuvo URL tras {max_intentos} intentos")
            return
        
        data_info = response.json()
        if not data_info:
            print(f"❌Sin URL de datos en respuesta para {municipio_name}")
            registrar_error_municipio(code=municipio_code,
                                       name=municipio_name,
                                       error= f"Sin URL de datos en respuesta tras {max_intentos} intentos")
            return
        data_url = data_info['datos']
        time.sleep(delay_seconds)

        # --- Descargar datos reales ---
        for intento in range(max_intentos):
            try:
                response_data = requests.get(data_url, timeout=timeout_seconds)
                if response_data.status_code == 200:
                    print(f"✅ Datos descargados para municipio {municipio_name}")
                    success = True
                    return {"municipio_codigo_aemet": municipio_code,
                            "nombre_municipio_ine": municipio_name,
                            "fecha_descarga_utc": datetime.datetime.utcnow().isoformat(),
                            "raw_aemet_data_json": response_data.json()
                        }
                    break
                elif response_data.status_code in [429, 500]:
                    espera = (2 ** intento) * delay_seconds
                    print(f"Error {response_data.status_code} en descarga de datos. Esperando {espera}s antes de reintentar...")
                    time.sleep(espera)
                else:
                    print(f"Error HTTP {response_data.status_code} al descargar datos")
                    break
            except (RemoteDisconnected, ConnectionError, requests.Timeout) as e:
                espera = (2 ** intento) * 2
                print(f"⚠️ {type(e).__name__} durante descarga. Reintentando en {espera}s...")
                time.sleep(espera)
        else:
            print(f"❌ No se pudo obtener/descargar los datos después de varios intentos")
            registrar_error_municipio(code=municipio_code,
                                       name=municipio_name,
                                       error= f"Falló descarga de datos")

    except Exception as e:
        print(f"Error inesperado en el intervalo: {str(e)}")
        registrar_error_municipio(code=municipio_code,
                                       name=municipio_name,
                                       error= str(e))

    finally:
        time.sleep(delay_seconds)
        if not success:
            print(f"Municipio {municipio_name} registrado en log de errores.")

def registrar_error_municipio(code, name, error):
    """Registra municipios fallidos en un log"""
    with open(LOG_MUNCIPIOS, "a", encoding="utf-8") as f:
        f.write(f"{code},{name},{error}\n")

def reintentar_errores_municipios(api_key):
    """
    Reintenta descargas de municipios que fallaron y están en el log.
    Devuelve lista de diccionarios con los datos descargados.
    """
    if not os.path.exists(LOG_MUNCIPIOS):
        print("✅ No hay errores pendientes en municipios.")
        return []

    list_of_raw_jsons = []
    # Mienstras el log exista, reintentar
    while os.path.exists(LOG_MUNCIPIOS):
        print("\n🔄 Reintentando descargas fallidas desde el log de municipios...")
        with open(LOG_MUNCIPIOS, "r", encoding="utf-8") as f:
            errores = [line.strip().split(",") for line in f.readlines() if line.strip()]

        if not errores:
            print("✅ No quedan municipios por reintentar. Descargas completas")
            os.remove(LOG_MUNCIPIOS)
            break

        print(f"\n🔄 Reintentando {len(errores)} municipios fallidos...")

        nuevos_errores = []
        for err in errores:
            if len(err)<2:
                print(f"Línea invalida en log: {err}")
                continue
            code, name = err[0], err[1]
            data = descargar_prediccion_municipio(api_key, code, name, data_type='diaria')
            if data:
                list_of_raw_jsons.append(data)
            else:
                nuevos_errores.append((code, name, "Reintento fallido"))

        # Reescribir log con los que siguen fallando
        if nuevos_errores:
            with open(LOG_MUNCIPIOS, "w", encoding="utf-8") as f:
                for code, name, msg_error in nuevos_errores:
                    f.write(f"{code},{name},{msg_error}\n")
        else:
            if os.path.exists(LOG_MUNCIPIOS):
                os.remove(LOG_MUNCIPIOS)

    return list_of_raw_jsons



data_type: str ='diaria'
if __name__ == "__main__":
    # --- Descarga para la lista fija ---
    print("\nIniciando descarga de datos RAW para municipios fijos...")
    list_of_raw_jsons = []  

    for code, nombre in MUNICIPIOS_FIJOS:
        print(f"\nObteniendo datos para {nombre} ({code})...")
        aemet_data = descargar_prediccion_municipio(AEMET_API_KEY, code, nombre, data_type="diaria")
        if aemet_data:
            list_of_raw_jsons.append(aemet_data)
        time.sleep(API_CALL_DELAY_SECONDS)

    # Reintentar si hubo fallos
    extra_data = reintentar_errores_municipios(AEMET_API_KEY)
    list_of_raw_jsons.extend(extra_data)    

    print(f"n\✅ Datos obtenidos para {len(list_of_raw_jsons)} municipios")

    # --- Crear DataFrame de Spark ---
    spark = utils.create_context()

    schema = StructType([
        StructField("municipio_codigo_aemet", StringType(), True),
        StructField("nombre_municipio_ine", StringType(), True),
        StructField("fecha_descarga_utc", StringType(), True),
        StructField("raw_aemet_data_json_str", StringType(), True),
        StructField("id", LongType(), True),
        StructField("version", DoubleType(), True)
    ])

    df_raw_aemet = spark.createDataFrame(
        [
            (
                item["municipio_codigo_aemet"],
                item["nombre_municipio_ine"],
                item["fecha_descarga_utc"],
                json.dumps(item["raw_aemet_data_json"], ensure_ascii=False),
                random.randint(1, 1000000),
                1.0
            )
            for item in list_of_raw_jsons
        ],
        schema=schema
    )

    #Mostra mensaje de lista de datos guaradados con exito
    print(f"\n DataFrame RAW creado con {df_raw_aemet.count()} registros")
    #df_raw_aemet.show(5, truncate=False)

    # --- Guardar en Iceberg ---
    landing_db = "landing_db"
    landing_table = f"aemet_prediccion_{data_type}"

    utils.append_iceberg_table(spark, df_raw_aemet, landing_db, landing_table)

    #mostrar datos guardados
    print(f"\n Datos guardados en tabla iceberg: spark_catalog.{landing_db}.{landing_table}")
    utils.read_iceberg_table(spark,db_name=landing_db,table_name=landing_table).show(12, truncate=False)    
    spark.stop()
