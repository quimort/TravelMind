import utils as utils
import requests, time
import json
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.functions import col, explode

def descargar_datos_aemet_raw(start_date, end_date, api_key, delay_seconds=0.5):
    """
    Descarga todos los datos climáticos de AEMET y los guarda en un único JSON.
    
    Args:
        start_date (datetime): Fecha de inicio
        end_date (datetime): Fecha de fin
        api_key (str): API key de AEMET
        delay_seconds (float): Pausa entre llamadas a la API
    
    Returns:
        str: Ruta del archivo JSON unificado guardado
    """
    
    # --------------------------------------------
    # 1. Configuración inicial
    # --------------------------------------------
    headers = {'api_key': api_key}
    base_url = 'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/'
    end_url = '/todasestaciones'
    all_climatological_data = []
    
    print(f" Iniciando descarga del período: {start_date.date()} a {end_date.date()}")

    # --------------------------------------------
    # 2. Generar intervalos de 15 días
    # --------------------------------------------
    def generar_intervalos(start, end):
        delta = end - start
        total_days = delta.days + 1
        interval_days = 15
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
    
    date_intervals = generar_intervalos(start_date, end_date)
    print(f"Se procesarán {len(date_intervals)} intervalos de 15 días")

    # --------------------------------------------
    # 3. Descargar todos los datos
    # --------------------------------------------
    for i, (start_str, end_str) in enumerate(date_intervals, 1):
        interval_url = f'{base_url}{start_str}/fechafin/{end_str}{end_url}'
        print(f"\nProcesando intervalo {i}/{len(date_intervals)}: {start_str[:10]} a {end_str[:10]}")

        try:
            # 3.1 Obtener URL de descarga
            response_url = requests.get(interval_url, headers=headers)
            time.sleep(delay_seconds * 0.3)
            
            if response_url.status_code != 200:
                print(f"Error en la solicitud (HTTP {response_url.status_code})")
                continue
                
            data_url = response_url.json().get('datos')
            if not data_url:
                print("No se encontró URL de datos en la respuesta")
                continue
            
            # 3.2 Descargar datos reales
            response_data = requests.get(data_url)
            if response_data.status_code == 200:
                datos_intervalo = response_data.json()
                all_climatological_data.extend(datos_intervalo)
                print(f"Descargados {len(datos_intervalo)} registros (Total acumulado: {len(all_climatological_data)})")
            else:
                print(f"Error al descargar datos (HTTP {response_data.status_code})")
            
        except Exception as e:
            print(f"Error en el intervalo: {str(e)}")
        
        finally:
            time.sleep(delay_seconds)

    # --------------------------------------------
    # 4. Guardar todo en un único JSON
    # --------------------------------------------
    if not all_climatological_data:
        print("\nNo se descargaron datos válidos")
        return None
    
    archivojson = f"aemet_unificado_{start_date.date()}_{end_date.date()}.json"
    
    datos_completos = {
        'metadata': {
            'fecha_inicio': start_date.isoformat(),
            'fecha_fin': end_date.isoformat(),
            'fecha_generacion': datetime.now().isoformat(),
            'total_registros': len(all_climatological_data),
            'total_estaciones': len({d['indicativo'] for d in all_climatological_data})
        },
        'data': all_climatological_data
    }
    
    with open(archivojson, 'w', encoding='utf-8') as f:
        json.dump(datos_completos, f, ensure_ascii=False, indent=2)
    
    print(f"\n Archivo guardado como: {archivojson}")
    print(f"Estadísticas:")
    print(f"- Registros totales: {len(all_climatological_data)}")
    print(f"- Estaciones únicas: {len({d['indicativo'] for d in all_climatological_data})}")
    
    return archivojson

pass    
# --------------------------------------------
# 1. Configura tu API Key
# Es mejor no hardcodear la clave API directamente en el código para la seguridad.
# Sin embargo, para este ejemplo, la asignaremos directamente.
# En un proyecto real, usarías variables de entorno.
API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJmcnZhcmdhcy44N0BnbWFpbC5jb20iLCJqdGkiOiI3MTJmNjFkYi1hMDg3LTRkM2QtODFlNS04ZjY4YjYwOWE2YTAiLCJpc3MiOiJBRU1FVCIsImlhdCI6MTc0OTIyOTY1OSwidXNlcklkIjoiNzEyZjYxZGItYTA4Ny00ZDNkLTgxZTUtOGY2OGI2MDlhNmEwIiwicm9sZSI6IiJ9.BbMqB0Jj2_z5wJw6luQhH7iMlJDMk2gfPEVOQ7Chc7E"

# 2. Define las fechas para la descarga
# Aquí un ejemplo para descargar los datos de los últimos 30 días
start_date = datetime(2020, 1, 1)
end_date = datetime(2020, 1, 30)

# 3. Llama a la función con los argumentos
# y maneja la creación del contexto Spark
if __name__ == "__main__":
    try:
        
        print("Iniciando la descarga de datos de AEMET...")
        # La función devuelve el nombre del archivo generado
        nombre_archivo_generado = descargar_datos_aemet_raw(start_date, end_date, API_KEY)
        
        # Verificar que el archivo existe
        if not os.path.exists(nombre_archivo_generado):
            raise FileNotFoundError(f"No se encontró el archivo {nombre_archivo_generado}")
        
        print(f"\nCargando archivo {nombre_archivo_generado} a Spark...")
        
        # Configuracion Spark
        # 1. Crear la sesión de Spark correctamente
        spark = utils.create_context()
        # 2. Obtener el SparkContext desde la SparkSession
        sc = spark.sparkContext

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
            StructField("hrMedia", StringType()),  # humedad relativa media
            StructField("hrMax", StringType()),  # humedad relativa máxima
            StructField("horaHrMax", StringType()),
            StructField("hrMin", StringType()),  # humedad relativa mínima
            StructField("horaHrMin", StringType())
        ])
        # 4. Cargar json y convertir a DataFrame
        df_raw = spark.read.option("multiline", "true").json(nombre_archivo_generado)

        # 5. Procesar Json con Explode
        # Usar explode para descomponer el JSON y forzando al squema definido
        print("\n Procesando JSON a Spark...")
        df_spark_aemet = df_raw.select(explode(col("data")).alias("row")) \
            .selectExpr("row.*") \
            .selectExpr("*") \
            .selectExpr(*[f"CAST({c} AS STRING)" for c in esquema.fieldNames()])
        
        # Mostrar resultados
        print("\n Datos cargados correctamente...")
        #print(f"\nTotal registros: {df_spark_aemet.count()}")
        #df_spark_aemet.show(5,truncate=False)
        #  Filtrar provincias de interés
        provincias_interes = ["MADRID", "BARCELONA", "SEVILLA", "ILLES BALEARS", "BALEARES", "ALICANTE", "VALENCIA"]

        dfspark_filtrado = df_spark_aemet.filter(col("provincia").isin(provincias_interes))

        print(f"\nFiltrado realizado. Registros después del filtro: {dfspark_filtrado.count()}")
        dfspark_filtrado.show(10, truncate=False)

        
        #         
        # 6. Guardar en Iceberg        
        # 6.1 Definir nombres de base de datos y tabla
        db_name = "landing_db"
        table_name = "aemetRawDiario"
        # 6.2 Guardar en Iceberg (usando función utils)
        print(f"\nGuardando datos en Iceberg: {db_name}.{table_name}")
        utils.create_iceberg_table(spark, dfspark_filtrado, db_name, table_name)
        # Eliminar el archivo JSON generado
        if os.path.exists(nombre_archivo_generado):
            os.remove(nombre_archivo_generado)
            print(f"Archivo {nombre_archivo_generado} eliminado correctamente.")
        else:
            print(f"Archivo {nombre_archivo_generado} no encontrado para eliminar.")

        # 6.3 Verificar ubicación de la tabla Iceberg
        print("\nUbicación de la tabla Iceberg:")
        #spark.sql("DESCRIBE FORMATTED local_db.aemetRawDiario").filter("col_name = 'Location'").show(truncate=False)
        location = (
        spark.sql(f"DESCRIBE FORMATTED {db_name}.{table_name}")
            #.filter(f"{db_name}.{table_name}")
            .filter("col_name = 'Location'")
            .select("data_type")
            .collect()[0][0]
        )
        print(f"La tabla Iceberg se guardó en: {location}")
       
    except Exception as e:
        print(f"\n Error: {str(e)}")
        raise e
    finally:
        # Limpieza con tiempo para evitar warnings
        time.sleep(3)
        if 'spark' in locals():
            spark.stop()