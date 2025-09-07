import utils as utils
import requests
import pandas as pd
import io
import json
import time
import datetime
import random
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType


# --- Configuración General ---
# IMPORTANTE: ¡Reemplaza con tu clave API de AEMET!
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

# URL del archivo XLSX del INE con los códigos de municipio
INE_XLSX_URL = 'https://www.ine.es/daco/daco42/codmun/diccionario25.xlsx'

# Código de municipio que queremos asegurar que siempre esté incluido
REQUIRED_MUNICIPALITY_CODE = '08019' # Barcelona

# Número de municipios aleatorios a seleccionar (además del obligatorio)
NUM_RANDOM_MUNICIPALITIES = 3 

# Retraso entre llamadas a la API de AEMET (en segundos)
API_CALL_DELAY_SECONDS = 0.5 

# --- Función para obtener datos de AEMET para un solo municipio (implementa el proceso de 2 pasos) ---
def get_aemet_prediction_for_municipio(api_key: str, municipality_code: str, data_type: str = 'diaria'):
    """
    Obtiene datos de predicción de la API de AEMET para un municipio y tipo de dato específico.
    Maneja el proceso de dos pasos de la API de AEMET.

    Args:
        api_key (str): Clave de API de AEMET.
        municipality_code (str): Código de municipio de AEMET (ej. '08019').
        data_type (str): 'diaria' para predicción diaria, 'horaria' para predicción horaria.

    Returns:
        dict or list: Los datos obtenidos en formato JSON, o None si ocurre un error.
    """
    if data_type not in ['diaria', 'horaria']:
        print(f"Advertencia: Tipo de dato '{data_type}' no soportado. Usando 'diaria'.")
        data_type = 'diaria'

    # PRIMER PASO: Llamada a la API de AEMET para obtener la URL de los datos
    initial_api_url = f'https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/{data_type}/{municipality_code}'
    
    headers = {
        'accept': 'application/json',
        'api_key': api_key # La API Key se pasa en los headers para esta primera llamada
    }

    try:
        initial_response = requests.get(initial_api_url, headers=headers)
        initial_response.raise_for_status() # Lanza una excepción para errores HTTP
        
        data_info = initial_response.json()

        if 'datos' in data_info:
            data_url = data_info['datos']
            time.sleep(API_CALL_DELAY_SECONDS) # Pequeña espera para no sobrecargar la API

            # SEGUNDO PASO: Llamada a la URL de 'datos' para obtener el JSON real
            # Aquí no se necesita la api_key en los headers, ya que es una URL directa
            final_data_response = requests.get(data_url)
            final_data_response.raise_for_status() 
            
            return final_data_response.json() # Devolvemos el JSON de la predicción
        else:
            print(f"  Error para {municipality_code}: No se encontró 'datos' en la primera respuesta. {data_info}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"  Error de solicitud para {municipality_code}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"    Estado HTTP: {e.response.status_code}, Contenido: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        print(f"  Error de JSON para {municipality_code}: {e}")
        return None
    except Exception as e:
        print(f"  Error inesperado para {municipality_code}: {e}")
        return None

# --- Función Principal para obtener JSON Crudo ---
def get_aemet_raw_json_for_selected_municipalities(
    api_key: str, 
    ine_xlsx_url: str, 
    required_mun_code: str, 
    num_random_mun: int, 
    data_type: str = 'diaria'
) -> list:
    """
    Lee códigos de municipio de un XLSX del INE, selecciona un conjunto de ellos (incluyendo uno obligatorio),
    y descarga los datos de predicción de AEMET en formato JSON CRUDO para cada uno.

    Args:
        api_key (str): Tu clave de API de AEMET.
        ine_xlsx_url (str): URL del archivo XLSX del INE.
        required_mun_code (str): Código del municipio que siempre debe incluirse.
        num_random_mun (int): Número de municipios aleatorios a seleccionar.
        data_type (str): Tipo de dato de predicción ('diaria' o 'horaria').

    Returns:
        list: Una lista de diccionarios. Cada diccionario contiene información del municipio
              y el JSON crudo de la predicción de AEMET.
              Devuelve una lista vacía si no se pudieron obtener datos.
    """
    
    # 1. Descargar y leer el XLSX del INE
    print(f"1. Descargando el archivo XLSX del INE desde: {ine_xlsx_url}")
    try:
        response = requests.get(ine_xlsx_url)
        response.raise_for_status()
        file_content = io.BytesIO(response.content)
        
        df_ine = pd.read_excel(file_content, header=1) 
        
        print("   Archivo XLSX del INE leído exitosamente.")
        print(f"   Columnas leídas del Excel: {df_ine.columns.tolist()}") 
        
    except Exception as e:
        print(f"Error al leer el archivo XLSX del INE: {e}")
        print("Asegúrate de que la URL es correcta y el archivo Excel es accesible y no corrupto.")
        return []

    # --- Preprocesar códigos de municipio del INE ---
    try:
        df_ine_filtered = df_ine.dropna(subset=['CPRO', 'CMUN'])
        df_ine_filtered['CPRO_STR'] = df_ine_filtered['CPRO'].astype(int).astype(str).str.zfill(2)
        df_ine_filtered['CMUN_STR'] = df_ine_filtered['CMUN'].astype(int).astype(str).str.zfill(3)
        df_ine_filtered['COD_AEMET'] = df_ine_filtered['CPRO_STR'] + df_ine_filtered['CMUN_STR']
    except KeyError as e:
        print(f"\n¡Error de columna después de leer el Excel! La columna {e} no se encontró en el DataFrame.")
        print(f"Columnas disponibles en el DataFrame: {df_ine.columns.tolist()}")
        print("A pesar de especificar 'header=1', las columnas 'CPRO' y 'CMUN' no se encontraron. Revisa si hay un error tipográfico en el código o si el nombre de las columnas en el Excel es diferente (mayúsculas/minúsculas, espacios, etc.).")
        return []
    except Exception as e:
        print(f"\nError al procesar los códigos de municipio después de leer el Excel: {e}")
        return []

    all_available_codes = df_ine_filtered['COD_AEMET'].unique().tolist()
    
    # 2. Seleccionar municipios
    selected_municipio_codes = set()
    
    if required_mun_code in all_available_codes:
        selected_municipio_codes.add(required_mun_code)
        print(f"2.1. Incluyendo el municipio obligatorio: {required_mun_code}")
    else:
        print(f"Advertencia: El municipio obligatorio {required_mun_code} no se encontró en el XLSX. Asegúrate de que el código sea correcto.")
    
    eligible_for_random = [code for code in all_available_codes if code not in selected_municipio_codes]
    if len(eligible_for_random) >= num_random_mun:
        random_selection = random.sample(eligible_for_random, num_random_mun)
        selected_municipio_codes.update(random_selection)
        print(f"2.2. Seleccionando {num_random_mun} municipios aleatorios.")
    else:
        print(f"Advertencia: No hay suficientes municipios para seleccionar {num_random_mun} aleatorios. Seleccionando todos los restantes ({len(eligible_for_random)}).")
        selected_municipio_codes.update(eligible_for_random)

    final_municipio_codes = list(selected_municipio_codes)
    print(f"3. Se descargarán datos para los siguientes municipios ({len(final_municipio_codes)} en total): {final_municipio_codes}")

    all_raw_aemet_data = []
    
    # 4. Iterar por cada municipio seleccionado y descargar datos de AEMET
    print("4. Iniciando descarga de datos RAW de AEMET para los municipios seleccionados...")
    for i, code in enumerate(final_municipio_codes):
        print(f"  [{i+1}/{len(final_municipio_codes)}] Obteniendo datos RAW para municipio {code}...")
        aemet_data = get_aemet_prediction_for_municipio(api_key, code, data_type=data_type)
        
        if aemet_data:
            municipio_info_row = df_ine_filtered[df_ine_filtered['COD_AEMET'] == code].iloc[0]
            
            # Almacenar el JSON crudo junto con la información del municipio
            raw_record = {
                'municipio_codigo_aemet': code,
                'nombre_municipio_ine': municipio_info_row.get('NOMBRE', None),
                'provincia_ine': municipio_info_row.get('PROVINCIA', None),
                'ccaa_ine': municipio_info_row.get('CA', None),
                'fecha_descarga_utc': datetime.datetime.utcnow().isoformat(),
                'raw_aemet_data_json': aemet_data # Aquí guardamos el JSON completo
            }
            all_raw_aemet_data.append(raw_record)
        
        time.sleep(API_CALL_DELAY_SECONDS) 

    if not all_raw_aemet_data:
        print("No se pudieron obtener datos RAW de AEMET para ningún municipio seleccionado.")
        return []

    print(f"\n5. Se han obtenido datos RAW de AEMET para {len(all_raw_aemet_data)} municipios.")
    return all_raw_aemet_data

list_of_raw_jsons = get_aemet_raw_json_for_selected_municipalities(
        api_key=AEMET_API_KEY,
        ine_xlsx_url=INE_XLSX_URL,
        required_mun_code=REQUIRED_MUNICIPALITY_CODE,
        num_random_mun=NUM_RANDOM_MUNICIPALITIES,
        data_type='diaria' #o si prefieres la predicción 'horaria'
    )

if list_of_raw_jsons:
        print("\n--- JSONs RAW Obtenidos ---")
        for i, item in enumerate(list_of_raw_jsons):
            print(f"\n--- Municipio {i+1}: {item['nombre_municipio_ine']} ({item['municipio_codigo_aemet']}) ---")
            # Imprime el JSON de forma legible
            print(json.dumps(item['raw_aemet_data_json'], indent=2, ensure_ascii=False))
            print("-" * 50) # Separador

        print(f"\nTotal de municipios con JSONs RAW: {len(list_of_raw_jsons)}")
else:
        print("No se obtuvieron JSONs RAW para ningún municipio.")

print("\nProceso finalizado. Ahora tienes una lista de diccionarios con el JSON crudo de AEMET para cada municipio seleccionado.")
print("Puedes usar esta lista (por ejemplo, 'list_of_raw_jsons') para crear un DataFrame de Spark o guardarla.")

#---Configuracion Spark y creacion DataFrame de Spark---
#crear context spark
spark = utils.create_context()

#creamos la estructura del DataFrame de Spark
# Definimos el esquema del DataFrame de Spark
schema = StructType([
    StructField("municipio_codigo_aemet", StringType(), True),
    StructField("nombre_municipio_ine", StringType(), True),
    StructField("fecha_descarga_utc", StringType(), True),
    StructField("raw_aemet_data_json_str", StringType(), True), # This should always be a string
    StructField("id", LongType(), True), # Use LongType as IDs might exceed IntegerType max
    StructField("version", DoubleType(), True)
])

#crear DataFrame de Spark a partir de la lista de diccionarios
df_raw_aemet = spark.createDataFrame(
    [
        (
            item.get('municipio_codigo_aemet'),
            item.get('nombre_municipio_ine'),
            item.get('fecha_descarga_utc'),
            json.dumps(item.get('raw_aemet_data_json'), ensure_ascii=False) if item.get('raw_aemet_data_json') else '[]',
            random.randint(1, 1000000),
            1.0
        )
        for item in list_of_raw_jsons
    ],
    schema=schema
)

df_raw_aemet.show(5, truncate=False)


db_name = "landing_db"
table_name = "aemet_prediccion_diaria"

utils.overwrite_iceberg_table(spark,df_raw_aemet,db_name,table_name)