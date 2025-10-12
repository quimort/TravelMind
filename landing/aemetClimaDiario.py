import utilsJoaquim_airflow as utils
import requests, time, json, os
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from http.client import RemoteDisconnected
from requests.exceptions import ConnectionError

LOG_FILE = "errores_clima.log"

# --------------------------------------------
# 1. Generar intervalos de días 
# --------------------------------------------
def generar_intervalos(start, end, interval_days=15):
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
    interval_url = f'{base_url}{start_date}/fechafin/{end_date}{end_url}'
    success = False

    try:
        for intento in range(max_intentos):
            try:
                response = requests.get(interval_url, headers=headers, timeout=timeout_seconds)
                if response.status_code == 200:
                    break
                elif response.status_code in [429, 500]:
                    espera = (2 ** intento) * delay_seconds
                    print(f"⚠️ Error {response.status_code}, esperando {espera}s antes de reintentar...")
                    time.sleep(espera)
                else:
                    print(f"Error HTTP {response.status_code}")
                    return
            except (RemoteDisconnected, ConnectionError, requests.Timeout) as e:
                espera = (2 ** intento) * 2
                print(f"⚠️ {type(e).__name__} — reintentando en {espera}s...")
                time.sleep(espera)
        else:
            errores_intervals.append({
                "start": start_date, "end": end_date, "error": "No se obtuvo URL tras varios intentos"
            })
            return

        data_url = response.json().get('datos')
        if not data_url:
            errores_intervals.append({
                "start": start_date, "end": end_date, "error": "Sin URL de datos"
            })
            return

        # Descargar datos reales
        for intento in range(max_intentos):
            try:
                response_data = requests.get(data_url, timeout=timeout_seconds)
                if response_data.status_code == 200:
                    datos_intervalo = response_data.json()
                    if isinstance(datos_intervalo, list):
                        all_climatological_data.extend([d for d in datos_intervalo if isinstance(d, dict)])
                    elif isinstance(datos_intervalo, dict):
                        if "indicativo" in datos_intervalo:
                            all_climatological_data.append(datos_intervalo)
                    success = True
                    break
                elif response_data.status_code in [429, 500]:
                    espera = (2 ** intento) * delay_seconds
                    print(f"Error {response_data.status_code}, reintentando en {espera}s...")
                    time.sleep(espera)
            except (RemoteDisconnected, ConnectionError, requests.Timeout) as e:
                espera = (2 ** intento) * 2
                print(f"⚠️ {type(e).__name__} durante descarga, reintentando en {espera}s...")
                time.sleep(espera)
        else:
            errores_intervals.append({"start": start_date, "end": end_date, "error": "Falló descarga de datos"})

    finally:
        time.sleep(delay_seconds)
        if not success:
            print(f"❌ Intervalo {start_date} - {end_date} falló y fue registrado.")


# --------------------------------------------
# 3. Guardar errores
# --------------------------------------------
def guardar_errores(errores_intervals):
    if errores_intervals:
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write("start_date,end_date,error_message\n")
            for err in errores_intervals:
                f.write(f"{err['start']},{err['end']},{err['error']}\n")
        print(f"⚠️ Log de errores guardado en {LOG_FILE}")
    else:
        print("✅ Sin errores durante la descarga.")


# --------------------------------------------
# 4. Guardar datos completos en JSON
# --------------------------------------------
def guardar_json(datos, start_date, end_date):
    datos = [d for d in datos if isinstance(d, dict) and 'indicativo' in d]
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
    print(f"✅ Archivo guardado como: {archivojson}")
    return archivojson


# --------------------------------------------
# 5. Cargar JSON a Iceberg
# --------------------------------------------
def cargar_json_a_iceberg(spark, archivojson, esquema, db_name, table_name, batch_size=2000, particiones=200):
    file_size_mb = os.path.getsize(archivojson) / (1024 * 1024)
    print(f"Tamaño del archivo: {file_size_mb:.2f} MB")

    with open(archivojson, "r", encoding="utf-8") as f:
        contenido = json.load(f)

    if file_size_mb > 500:
        años = list({r["fecha"][:4] for r in contenido["data"]})
        for y in años:
            registros = [r for r in contenido["data"] if r["fecha"].startswith(str(y))]
            for i in range(0, len(registros), batch_size):
                bloque = registros[i:i+batch_size]
                df = spark.createDataFrame(bloque, schema=esquema)
                df = (
                    df.withColumn("fecha_dt", F.to_date("fecha", "yyyy-MM-dd"))
                      .withColumn("year", F.year("fecha_dt"))
                      .withColumn("month", F.month("fecha_dt"))
                      .repartition(particiones, "year", "month")
                )
                utils.append_iceberg_table(spark, df, db_name, table_name)
    else:
        df = spark.createDataFrame(contenido["data"], schema=esquema)
        df = (
            df.withColumn("fecha_dt", F.to_date("fecha", "yyyy-MM-dd"))
              .withColumn("year", F.year("fecha_dt"))
              .withColumn("month", F.month("fecha_dt"))
        )
        utils.append_iceberg_table(spark, df, db_name, table_name)

    location = (
        spark.sql(f"DESCRIBE FORMATTED {db_name}.{table_name}")
        .filter("col_name = 'Location'")
        .select("data_type").collect()[0][0]
    )
    print(f"✅ Tabla Iceberg guardada en: {location}")


# --------------------------------------------
# 6. Función principal del proceso AEMET
# --------------------------------------------
def process_aemet_clima_diario():
    API_KEY = os.getenv("AEMET_API_KEY", "TU_API_KEY_AQUI")
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2024, 12, 31)
    headers = {'api_key': API_KEY}

    all_data = []
    errores = []
    intervals = generar_intervalos(start_date, end_date)
    print(f"🔄 Descargando {len(intervals)} intervalos de datos AEMET...")

    for idx, (start_str, end_str) in enumerate(intervals, start=1):
        print(f"→ Intervalo {idx}/{len(intervals)}: {start_str[:10]} a {end_str[:10]}")
        descargar_intervalo(start_str, end_str, headers, errores, all_data)

    guardar_errores(errores)
    if not all_data:
        print("❌ No se descargaron datos válidos.")
        return

    archivojson = guardar_json(all_data, start_date, end_date)

    # Crear sesión Spark
    spark = utils.create_context()
    print("⚙️ Spark session creada correctamente.")

    esquema = StructType([
        StructField("fecha", StringType()),
        StructField("indicativo", StringType()),
        StructField("nombre", StringType()),
        StructField("provincia", StringType()),
        StructField("altitud", StringType()),
        StructField("tmed", StringType()),
        StructField("prec", StringType()),
        StructField("tmin", StringType()),
        StructField("tmax", StringType()),
        StructField("sol", StringType()),
        StructField("racha", StringType()),
    ])

    cargar_json_a_iceberg(spark, archivojson, esquema, db_name="landing", table_name="aemetClimaDiario")
    print("✅ Proceso completo AEMET finalizado correctamente.")
    spark.stop()


# --------------------------------------------
# Permitir ejecución directa o desde Airflow
# --------------------------------------------
if __name__ == "__main__":
    process_aemet_clima_diario()
