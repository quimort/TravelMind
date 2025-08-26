import utils as utils
import requests
import json
import time
import datetime
import random
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# --- Configuración General ---
AEMET_API_KEY = 'eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJmcnZhcmdhcy44N0BnbWFpbC5jb20iLCJqdGkiOiI3MTJmNjFkYi1hMDg3LTRkM2QtODFlNS04ZjY4YjYwOWE2YTAiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc0OTIyOTY1OSwidXNlcklkIjoiNzEyZjYxZGItYTA4Ny00ZDNkLTgxZTUtOGY2OGI2MDlhNmEwIiwicm9sZSI6IiJ9.BbMqB0Jj2_z5wJw6luQhH7iMlJDMk2gfPEVOQ7Chc7E'


# Lista fija de municipios que quieres consultar
MUNICIPIOS_FIJOS = [
    ("28079", "Madrid"),
    ("08019", "Barcelona"),
    ("41091", "Sevilla"),
    ("07040", "Palma de Mallorca"),
    ("03031", "Benidorm"),
    ("46250", "Valencia")
]

API_CALL_DELAY_SECONDS = 0.5 
data_type: str ='diaria'
# --- Función para obtener datos de AEMET ---
def get_aemet_prediction_for_municipio(api_key: str, municipio: str, data_type: str , max_retries=3):
    """
    Descarga la predicción AEMET para un municipio.
    data_type puede ser 'diaria' o 'horaria'.
    """

    # Validar tipo
    if data_type not in ["diaria", "horaria"]:
        raise ValueError("data_type debe ser 'diaria' o 'horaria'")

    url_inicial = f"https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/{data_type}/{municipio}"
    headers = {"accept": "application/json", "api_key": api_key}

    for intento in range(max_retries):
        try:
            resp = requests.get(url_inicial, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            # Verificar si existe 'datos'
            if "datos" not in data:
                return None, f"No se encontró 'datos' en respuesta inicial (estado={data.get('estado')})"

            datos_url = data["datos"]
            time.sleep(1)  # para no saturar la API

            resp_final = requests.get(datos_url, timeout=10)
            resp_final.raise_for_status()
            return resp_final.json(), None

        except Exception as e:
            if intento < max_retries - 1:
                print(f"   Reintento {intento+1} para {municipio} tras error: {e}")
                time.sleep(2)
            else:
                return None, str(e)

# --- Descarga para la lista fija ---
print("\nIniciando descarga de datos RAW para municipios fijos...")
list_of_raw_jsons = []

for code, nombre in MUNICIPIOS_FIJOS:
    print(f"\nObteniendo datos para {nombre} ({code})...")
    aemet_data = get_aemet_prediction_for_municipio(AEMET_API_KEY, code, data_type="diaria")

    if aemet_data:
        raw_record = {
            "municipio_codigo_aemet": code,
            "nombre_municipio_ine": nombre,
            "fecha_descarga_utc": datetime.datetime.utcnow().isoformat(),
            "raw_aemet_data_json": aemet_data
        }
        list_of_raw_jsons.append(raw_record)
    time.sleep(API_CALL_DELAY_SECONDS)

print(f"✅ Datos obtenidos para {len(list_of_raw_jsons)} municipios")

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
db_name = "landing_db"
table_name = f"aemet_prediccion_{data_type}"

utils.overwrite_iceberg_table(spark, df_raw_aemet, db_name, table_name)

spark.stop()
