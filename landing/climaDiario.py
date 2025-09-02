import utils as utils
import requests, time
import json
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col, explode
from http.client import RemoteDisconnected
from requests.exceptions import ConnectionError

# --- Configuración global ---
LOG_FILE = "errores_clima.log"

# --------------------------------------------
# 1. Generar intervalos de días 
# --------------------------------------------
def generar_intervalos(start, end, interval_days = 15):
    delta = end - start
    total_days = delta.days + 1
    num_intervals = (total_days + interval_days - 1) // interval_days
    intervals = []
    current_start = start
    
    for _ in range(num_intervals):
        current_end = min(current_start + timedelta(days=interval_days-1), end)
        intervals.append((
            current_start.strftime('%Y-%m-%dT%H:%M:%SUTC'),
            current_end.strftime('%Y-%m-%dT%H:%M:%SUTC')
        ))
        current_start = current_end + timedelta(days=1)
    
    return intervals
# --------------------------------------------
# 2. Descargar un intervalo con reintentos
# --------------------------------------------
def descargar_intervalo(start_date, end_date, headers, errores_intervals, 
                        all_climatological_data, max_intentos=5, delay_seconds=5, timeout_seconds=5):
    base_url = 'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/'
    end_url = '/todasestaciones'
    #all_climatological_data = []
    interval_url = f'{base_url}{start_date}/fechafin/{end_date}{end_url}'
    # Convertir strings a datetime solo para los prints
    start_dt = datetime.strptime(start_date[:10], "%Y-%m-%d") if isinstance(start_date, str) else start_date
    end_dt = datetime.strptime(end_date[:10], "%Y-%m-%d") if isinstance(end_date, str) else end_date

    print(f"\nIniciando descarga del período: {start_dt.date()} a {end_dt.date()}")
    success = False
    try:
        # --- Obtener URL de descarga ---
        for intento in range(max_intentos):
            try:
                response = requests.get(interval_url, headers=headers, timeout=timeout_seconds)
                if response.status_code == 200:
                    break
                elif response.status_code in [429, 500]:
                    espera = (2 ** intento) * delay_seconds
                    print(f"⚠️ Error HTTP {response.status_code}. Esperando {espera}s antes de reintentar nuevamente...")
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
            errores_intervals.append({
                "start": start_date,
                "end": end_date,
                "error": f"No se obtuvo URL tras {max_intentos} intentos"
            })
            return
        data_url = response.json().get('datos')
        if not data_url:
            print("❌ No se encontró URL de datos en la respuesta")
            errores_intervals.append({
                "start": start_date,
                "end": end_date,
                "error": "Sin URL de datos en respuesta"
            })
            return

        # --- Descargar datos reales ---
        for intento in range(max_intentos):
            try:
                response_data = requests.get(data_url, timeout=timeout_seconds)
                if response_data.status_code == 200:
                    datos_intervalo = response_data.json()
                    all_climatological_data.extend(datos_intervalo)
                    print(f"✅ Descargados {len(datos_intervalo)} registros (Total acumulado: {len(all_climatological_data)})")
                    success = True
                    break
                elif response_data.status_code in [429, 500]:
                    espera = (2 ** intento) * delay_seconds
                    print(f"Error {response_data.status_code} en descarga. Esperando {espera}s antes de reintentar...")
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
            errores_intervals.append({"start": start_date, "end": end_date, "error": "Falló descarga de datos"})

    except Exception as e:
        print(f"Error inesperado en el intervalo: {str(e)}")
        errores_intervals.append({"start": start_date, "end": end_date, "error": str(e)})

    finally:
        time.sleep(delay_seconds)
        if not success:
            print(f"Intervalo {start_date} a {end_date} registrado en log de errores.")
        
# --------------------------------------------
# 3. Guardar errores en archivo
# --------------------------------------------
def guardar_errores(errores_intervals):
    if errores_intervals:
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("start_date,end_date,error_message\n")
            for err in errores_intervals:
                f.write(f"{err['start']},{err['end']},{err['error']}\n")
        print(f"\n⚠️ Se guardó log de errores en: {LOG_FILE}")
    else:
        print("\n✅ No hubo errores en la descarga")

# --------------------------------------------
# 4. Guardar datos completos en JSON
# --------------------------------------------
def guardar_json(datos, start_date, end_date):
    archivojson = f"aemet_unificado_{start_date.date()}_{end_date.date()}.json"
    datos_completos = {
        'metadata': {
            'fecha_inicio': start_date.isoformat(),
            'fecha_fin': end_date.isoformat(),
            'fecha_generacion': datetime.now().isoformat(),
            'total_registros': len(datos),
            'total_estaciones': len({d['indicativo'] for d in datos})
        },
        'data': datos
    }
    with open(archivojson, 'w', encoding='utf-8') as f:
        json.dump(datos_completos, f, ensure_ascii=False, indent=2)
    print(f"\nArchivo guardado como: {archivojson}")
    return archivojson

# --------------------------------------------
# 5. Reintentar intervalos fallidos
# --------------------------------------------
# Funcion reintentar automáticamente errores indefinidamente(hasta que no haya errores de descarga)---
def reintentar_errores(api_key: str):
    """
    Reintenta automáticamente la descarga de intervalos fallidos registrados en el log de errores.
    Termina cuando no queden errores pendientes.
    """
    archivos_generados = []
    headers = {'api_key': api_key}

    # --- Verificación inicial ---
    if not os.path.exists(LOG_FILE):
        print("\n✅ No hay archivo de errores, nada que reintentar.")
        return archivos_generados

    while True:
        print("\n🔄 Reintentando descargas fallidas desde el log...")

        # Leer log de errores
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            next(f)  # Saltar cabecera
            errores = [line.strip().split(",") for line in f.readlines()]

        if not errores:
            print("✅ No quedan errores por reintentar. Descargas completas.")
            break

        print(f"⚠️ Se encontraron {len(errores)} intervalos pendientes. Reintentando...")

        nuevos_errores = []
        for start_str, end_str, _ in errores:
            all_data = []
            # Pasamos todos los argumentos requeridos
            descargar_intervalo(start_str, end_str, headers, nuevos_errores, all_data)

            if all_data:
                start_date = datetime.strptime(start_str[:10], "%Y-%m-%d")
                end_date = datetime.strptime(end_str[:10], "%Y-%m-%d")
                archivo_json = guardar_json(all_data, start_date, end_date)
                archivos_generados.append(archivo_json)

        # Guardar errores actualizados si hay fallos restantes
        if nuevos_errores:
            guardar_errores(nuevos_errores)
        else:
            print("✅ Todos los intervalos pendientes se descargaron correctamente.")
            # Borrar log si ya no quedan errores
            if os.path.exists(LOG_FILE):
                os.remove(LOG_FILE)
            break

    return archivos_generados


# --------------------------------------------
# 6. Cargar JSON a SPARK/Iceberg con particionado o sin particionado
# --------------------------------------------
def cargar_json_a_iceberg(spark, archivojson, esquema, db_name, table_name, batch_size=2000, particiones=200):
    # --- Ver tamaño del archivo ---
    file_size_mb = os.path.getsize(archivojson) / (1024 * 1024)
    print(f"Tamaño del archivo: {file_size_mb:.2f} MB")

    # --- Leer JSON completo (si no da OutOfMemory) ---
    print("\nCargando JSON en memoria...")
    with open(archivojson, "r", encoding="utf-8") as f:
        contenido = json.load(f)

    # --- Modo 1: Archivo grande (procesar por bloques y particionar) ---
    if file_size_mb > 500:
        años = list({r["fecha"][:4] for r in contenido["data"]})
        print("Archivo >500MB: procesando por bloques y particionando...")
        for y in años:
            print(f"\nProcesando año: {y}")
            registros_año = [r for r in contenido["data"] if r["fecha"].startswith(str(y))]

            if not registros_año:
                print(f"No hay datos para {y}")
                continue

            # Procesar en bloques para no saturar memoria
            for i in range(0, len(registros_año), batch_size):
                bloque = registros_año[i:i+batch_size]
                df_bloque = spark.createDataFrame(bloque, schema=esquema)
                # Añadir columnas de partición
                df_bloque = df_bloque.withColumn("fecha_dt", F.to_date("fecha","yyyy-MM-dd"))\
                            .withColumn("year", F.year("fecha_dt"))\
                            .withColumn("month", F.month("fecha_dt"))\
                            .repartition(particiones, "year", "month")  # particionado físico
                # Guardar en Iceberg con append
                utils.append_iceberg_table(spark, df_bloque, db_name, table_name)

        print(f"Año {y} agregado a Iceberg correctamente.")
    else:
        print("Archivo <=500MB: cargando todo y guardando sin particionar...")
        #crear DataFrame directamente
        df = spark.createDataFrame(contenido["data"], schema=esquema)
        df = (
            df.withColumn("fecha_dt", F.to_date("fecha", "yyyy-MM-dd"))
            .withColumn("year", F.year("fecha_dt"))
            .withColumn("month", F.month("fecha_dt"))
        )
        utils.append_iceberg_table(spark, df, db_name, table_name)
        print("\nDatos agregados en una sola carga.")
    # --- Verificar ubicación de la tabla Iceberg ---
    location = (
    spark.sql(f"DESCRIBE FORMATTED {db_name}.{table_name}")
    #.filter(f"{db_name}.{table_name}")
    .filter("col_name = 'Location'")
    .select("data_type")
    .collect()[0][0]
    )
    print(f"\nLa tabla Iceberg se guardó en: {location}")
    
pass    
# --------------------------------------------
# MAIN
# --------------------------------------------

# 3. Llama a la función con los argumentos
# y maneja la creación del contexto Spark
if __name__ == "__main__":
    # 1. Configura tu API Key
# Es mejor no hardcodear la clave API directamente en el código para la seguridad.
# Sin embargo, para este ejemplo, la asignaremos directamente.
# En un proyecto real, usarías variables de entorno.
    API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJmcnZhcmdhcy44N0BnbWFpbC5jb20iLCJqdGkiOiI3MTJmNjFkYi1hMDg3LTRkM2QtODFlNS04ZjY4YjYwOWE2YTAiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc0OTIyOTY1OSwidXNlcklkIjoiNzEyZjYxZGItYTA4Ny00ZDNkLTgxZTUtOGY2OGI2MDlhNmEwIiwicm9sZSI6IiJ9.BbMqB0Jj2_z5wJw6luQhH7iMlJDMk2gfPEVOQ7Chc7E"

    # 2. Define las fechas para la descarga
    # Aquí un ejemplo para descargar los datos de los últimos 30 días
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 2, 28)
    headers = {'api_key': API_KEY}
    all_climatological_data = []
    errores_intervals = []

    # Generar intervalos
    date_intervals = generar_intervalos(start_date, end_date)
    print("\nIniciando la descarga de datos de AEMET...")
    print(f"\nSe procesarán {len(date_intervals)} intervalos de 15 días")

    # Descargar todos los intervalos
    for start_str, end_str in date_intervals:
        descargar_intervalo(start_str, end_str, headers, errores_intervals, all_climatological_data)

    # Guardar errores y JSON
    guardar_errores(errores_intervals)
    if all_climatological_data:
        archivojson = guardar_json(all_climatological_data, start_date, end_date)
    else:
        print("No se descargaron datos válidos")
        archivojson = None
    
    # Cargar a Spark/Iceberg
    if archivojson and os.path.exists(archivojson):
        # Configuracion Spark
        # 1. Crear la sesión de Spark correctamente
        spark = utils.create_context()
        # Ver el warehouse configurado (ubicación en caso de necesitarlo, se puede usar para verificacion)
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
            StructField("sol", StringType()),  # radiación solar 
            StructField("presMax", StringType()),  # presión máxima
            StructField("horaPresMax", StringType()),
            StructField("presMin", StringType()),  # presión mínima
            StructField("horaPresMin", StringType()),
            StructField("hrMedia", StringType()),  # humedad relativa media
            StructField("hrMax", StringType()),  # humedad relativa máxima
            StructField("horaHrMax", StringType()),
            StructField("hrMin", StringType()),  # humedad relativa mínima
            StructField("horaHrMin", StringType())
        ])
        # 4. Cargar el JSON PRINCIPAL a Iceberg
        if archivojson and os.path.exists(archivojson):
            cargar_json_a_iceberg(spark, archivojson, esquema, db_name="landing_db", table_name="aemetClimaDiario", batch_size=2000, particiones=200)

        # 5. Reintentar errores automáticamente y cargar a Iceberg
        archivos_error_generados = reintentar_errores(API_KEY)
        if archivos_error_generados:
            for archivo in archivos_error_generados:
                cargar_json_a_iceberg(spark, archivo, esquema, db_name="landing_db", table_name="aemetClimaDiario")