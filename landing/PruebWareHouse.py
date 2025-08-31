import utils as utils
import time, requests, os, json
from datetime import datetime
from http.client import RemoteDisconnected
from requests.exceptions import ConnectionError
from pyspark.sql import functions as F
from pyspark.sql.types import *

LOG_FILE = "errores_clima2.log"
TEMP_JSON = "temp_datos_fallidos.json"  # Archivo temporal para cargar en Iceberg

# --------------------------------------------------
# DESCARGAR INTERVALO
# --------------------------------------------------
def descargar_intervalo(start_date, end_date, headers, errores_intervals, 
                        all_climatological_data, max_intentos=5, delay_seconds=5, timeout_seconds=5):
    base_url = 'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/'
    end_url = '/todasestaciones'
    interval_url = f'{base_url}{start_date}/fechafin/{end_date}{end_url}'

    start_dt = datetime.strptime(start_date[:10], "%Y-%m-%d")
    end_dt = datetime.strptime(end_date[:10], "%Y-%m-%d")

    print(f"\nIniciando descarga del período: {start_dt.date()} a {end_dt.date()}")
    success = False
    try:
        # --- Obtener URL de datos ---
        for intento in range(max_intentos):
            try:
                response = requests.get(interval_url, headers=headers, timeout=timeout_seconds)
                if response.status_code == 200:
                    break
                elif response.status_code in [429, 500]:
                    espera = (2 ** intento) * delay_seconds
                    print(f"⚠️ Error HTTP {response.status_code}. Esperando {espera}s antes de reintentar...")
                    time.sleep(espera)
                else:
                    print(f"Error HTTP {response.status_code} al obtener URL")
                    return
            except (RemoteDisconnected, ConnectionError, requests.Timeout) as e:
                espera = (2 ** intento) * 2
                print(f"⚠️ {type(e).__name__} durante descarga URL. Reintentando en {espera}s...")
                time.sleep(espera)
        else:
            print(f"❌ No se pudo obtener URL tras {max_intentos} intentos")
            errores_intervals.append({"start": start_date, "end": end_date, "error": "Sin URL"})
            return

        data_url = response.json().get('datos')
        if not data_url:
            print("❌ No se encontró URL de datos")
            errores_intervals.append({"start": start_date, "end": end_date, "error": "Sin URL de datos"})
            return

        # --- Descargar datos reales ---
        for intento in range(max_intentos):
            try:
                data_resp = requests.get(data_url, timeout=timeout_seconds)
                if data_resp.status_code == 200:
                    datos = data_resp.json()
                    all_climatological_data.extend(datos)
                    print(f"✅ Descargados {len(datos)} registros (Total acumulado: {len(all_climatological_data)})")
                    success = True
                    break
                elif data_resp.status_code in [429, 500]:
                    espera = (2 ** intento) * delay_seconds
                    print(f"⚠️ Error {data_resp.status_code} al descargar datos. Esperando {espera}s...")
                    time.sleep(espera)
                else:
                    print(f"Error HTTP {data_resp.status_code} al descargar datos")
                    break
            except (RemoteDisconnected, ConnectionError, requests.Timeout) as e:
                espera = (2 ** intento) * 2
                print(f"⚠️ {type(e).__name__} durante descarga. Reintentando en {espera}s...")
                time.sleep(espera)
        else:
            print(f"❌ No se pudo descargar datos tras {max_intentos} intentos")
            errores_intervals.append({"start": start_date, "end": end_date, "error": "Falló descarga"})

    except Exception as e:
        print(f"❌ Error inesperado: {str(e)}")
        errores_intervals.append({"start": start_date, "end": end_date, "error": str(e)})

    finally:
        time.sleep(delay_seconds)
        if not success:
            print(f"Intervalo {start_date} a {end_date} registrado en log de errores.")


# --------------------------------------------------
# GUARDAR JSON
# --------------------------------------------------
def guardar_json(datos, start_date, end_date):
    archivojson = f"aemet_unificado_{start_date.date()}_{end_date.date()}.json"
    contenido = {
        "metadata": {
            "fecha_inicio": start_date.isoformat(),
            "fecha_fin": end_date.isoformat(),
            "fecha_generacion": datetime.now().isoformat(),
            "total_registros": len(datos),
            "total_estaciones": len({d['indicativo'] for d in datos})
        },
        "data": datos
    }
    with open(archivojson, "w", encoding="utf-8") as f:
        json.dump(contenido, f, ensure_ascii=False, indent=2)
    print(f"\nArchivo guardado como: {archivojson}")
    return archivojson


# --------------------------------------------------
# REINTENTAR ERRORES
# --------------------------------------------------
def reintentar_errores(api_key):
    archivos_generados = []
    if not os.path.exists(LOG_FILE):
        print("\n✅ No hay archivo de errores, nada que reintentar.")
        return archivos_generados
    
    print("\n🔄 Reintentando descargas fallidas desde el log...")
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        next(f)
        errores = [line.strip().split(",") for line in f.readlines()]
    
    if not errores:
        print("✅ El log de errores está vacío.")
        return archivos_generados
    
    headers = {'api_key': api_key}
    for start_str, end_str, _ in errores:
        all_data = []
        descargar_intervalo(start_str, end_str, headers, [], all_data)
        start_date = datetime.strptime(start_str[:10], "%Y-%m-%d")
        end_date = datetime.strptime(end_str[:10], "%Y-%m-%d")
        if all_data:
            archivo = guardar_json(all_data, start_date, end_date)
            archivos_generados.append(archivo)
    return archivos_generados


# --------------------------------------------------
# CARGAR JSON A ICEBERG
# --------------------------------------------------
def cargar_json_a_iceberg(spark, archivojson, esquema, db_name, table_name, batch_size=2000, particiones=200):
    file_size_mb = os.path.getsize(archivojson) / (1024 * 1024)
    print(f"Tamaño del archivo: {file_size_mb:.2f} MB")

    print("\nLeyendo JSON...")
    with open(archivojson, "r", encoding="utf-8") as f:
        contenido = json.load(f)

    if file_size_mb > 500:
        años = list({r["fecha"][:4] for r in contenido["data"]})
        print("Archivo grande: procesando por bloques...")
        for y in años:
            registros = [r for r in contenido["data"] if r["fecha"].startswith(str(y))]
            for i in range(0, len(registros), batch_size):
                bloque = registros[i:i+batch_size]
                df = spark.createDataFrame(bloque, schema=esquema)
                df = df.withColumn("fecha_dt", F.to_date("fecha", "yyyy-MM-dd")) \
                       .withColumn("year", F.year("fecha_dt")) \
                       .withColumn("month", F.month("fecha_dt")) \
                       .repartition(particiones, "year", "month")
                utils.append_iceberg_table(spark, df, db_name, table_name)
    else:
        print("Archivo pequeño: cargando completo...")
        df = spark.createDataFrame(contenido["data"], schema=esquema)
        df = df.withColumn("fecha_dt", F.to_date("fecha", "yyyy-MM-dd")) \
               .withColumn("year", F.year("fecha_dt")) \
               .withColumn("month", F.month("fecha_dt"))
        utils.append_iceberg_table(spark, df, db_name, table_name)
    print("✅ Datos cargados en Iceberg.")


# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":
    API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJmcnZhcmdhcy44N0BnbWFpbC5jb20iLCJqdGkiOiI3MTJmNjFkYi1hMDg3LTRkM2QtODFlNS04ZjY4YjYwOWE2YTAiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc0OTIyOTY1OSwidXNlcklkIjoiNzEyZjYxZGItYTA4Ny00ZDNkLTgxZTUtOGY2OGI2MDlhNmEwIiwicm9sZSI6IiJ9.BbMqB0Jj2_z5wJw6luQhH7iMlJDMk2gfPEVOQ7Chc7E"
    archivos = reintentar_errores(API_KEY)

    if archivos:
        # Unir todos los datos descargados en un único archivo temporal
        all_data = []
        for archivo in archivos:
            with open(archivo, "r", encoding="utf-8") as f:
                contenido = json.load(f)
                all_data.extend(contenido["data"])

        temp_contenido = {
            "metadata": {
                "fecha_generacion": datetime.now().isoformat(),
                "total_registros": len(all_data),
                "total_estaciones": len({d['indicativo'] for d in all_data})
            },
            "data": all_data
        }

        with open(TEMP_JSON, "w", encoding="utf-8") as f:
            json.dump(temp_contenido, f, ensure_ascii=False, indent=2)

        print(f"\n📂 Archivo temporal creado: {TEMP_JSON}")

        spark = utils.create_context()
        esquema = StructType([
            StructField("fecha", StringType()),
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
            StructField("dir", StringType()),
            StructField("velmedia", StringType()),
            StructField("racha", StringType()),
            StructField("horaracha", StringType()),
            StructField("hrMedia", StringType()),
            StructField("hrMax", StringType()),
            StructField("horaHrMax", StringType()),
            StructField("hrMin", StringType()),
            StructField("horaHrMin", StringType())
        ])

        cargar_json_a_iceberg(spark, TEMP_JSON, esquema, db_name="landing_db", table_name="aemetClimaDiario")
        os.remove(TEMP_JSON)
        print("🗑️ Archivo temporal eliminado.")
    else:
        print("✅ No hay datos fallidos para cargar en Iceberg.")
