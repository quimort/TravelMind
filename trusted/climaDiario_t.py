from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DoubleType, IntegerType
import utils as utils
from pyspark.sql import functions as F
from pyspark.sql.functions import upper
from pyspark.sql.types import *


# --------------------------
# Función para reemplazar comas por puntos en columnas string numéricas
# --------------------------
def replace_commas_with_dots(df, columns):
    """
    Reemplaza ',' por '.' en columnas especificadas y convierte a DoubleType.
    """
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, regexp_replace(col(c), ",", ".").cast(DoubleType()))
    return df

if __name__ == "__main__":
    # 1. Crear sesión Spark
    spark = utils.create_context()

    # Tablas (ojo al catálogo)
    # Ajusta estos nombres según tu configuración
    db_trusted = "trusted_db"
    tbl_trusted = "aemetClimaDiarioTrusted"

    #nombre de la base de datos y tabla raw
    landing_db = "landing_db"
    tbl_landing = "aemetClimaDiario"
    print(f" Leyendo tabla RAW: spark_catalog.{landing_db}.{tbl_landing}")
    df_landing= utils.read_iceberg_table(spark,db_name=landing_db,table_name=tbl_landing)

    #mostrar 1 filas de tabla landing
    print("Mostrando 2 fila de LANDING:")
    df_landing.show(2, truncate=False)
    #cantidad de registros landind
    print(f"Cantidad de registros en LANDING: {df_landing.count()}")

    data_estaciones_objetivo= [
    ("8036Y", "Benidorm", "Benidorm", "Alacant/Alicante", 70, "383306N", "000800W"),
    ("0201D", "Barcelona", "Barcelona", "Barcelona", 6, "412326N", "021200E"),
    ("0201X", "Barcelona, Museo Marítimo", "Barcelona", "Barcelona", 5, "412230N", "021026E"),
    ("B228", "Palma de Mallorca, Puerto", "Palma de Mallorca", "Illes Balears", 3, "393312N", "023731E"),
    ("B236C", "Palma de Mallorca, Universidad", "Palma de Mallorca", "Illes Balears", 95, "393832N", "023837E"),
    ("B278", "Palma de Mallorca, Aeropuerto", "Palma de Mallorca", "Illes Balears", 8, "393339N", "024412E"),
    ("3126Y", "Madrid, El Goloso", "Madrid", "Madrid", 740, "403341N", "034243W"),
    ("3129", "Madrid Aeropuerto", "Madrid", "Madrid", 609, "402800N", "033320W"),
    ("3194U", "Madrid, Ciudad Universitaria", "Madrid", "Madrid", 664, "402706N", "034327W"),
    ("3195", "Madrid, Retiro", "Madrid", "Madrid", 667, "402443N", "034041W"),
    ("5783", "Sevilla Aeropuerto", "Sevilla", "Sevilla", 34, "372500N", "055245W"),
    ("5790Y", "Sevilla, Tablada", "Sevilla", "Sevilla", 9, "372151N", "060021W"),
    ("8416Y", "Valencia", "Valencia", "Comunitat Valenciana", 11, "392850N", "002159W"),
    ("8416X", "Valencia, UPV", "València", "Comunitat Valenciana", 6, "392847N", "002013W")
    ]
    
    schema = StructType([
    StructField("id", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("provincia", StringType(), True),
    StructField("altura", IntegerType(), True),
    StructField("latitud", StringType(), True),
    StructField("longitud", StringType(), True)
    ])
    #   Crear DataFrame de estaciones objetivo
    df_estaciones = spark.createDataFrame(data_estaciones_objetivo, schema)
    
    # ---- 2. Normalizar nombres de municipios a mayúsculas ----
    #df_estaciones = df_estaciones.withColumn("municipio", upper(df_estaciones["municipio"]))

    # ---- 3. Seleccionar columnas relevantes de estaciones ----
    df_estaciones_select = df_estaciones.select(
        F.col("id").alias("id"),
        F.col("municipio").alias("municipio"),
        F.col("provincia").alias("provincia"),
        F.col("latitud").alias("latitud"),
        F.col("longitud").alias("longitud")
    )
      
    # ---- 4. Filtrar landing solo IDs de interés ----
    id_list = [row.id for row in df_estaciones_select.select("id").collect()]
    landing_filtered = df_landing.filter(F.col("indicativo").isin(id_list))

    #seleccionar columnas de landing que estan duplicadas
    landing_filtered= landing_filtered.select("indicativo","nombre","altitud",
                                              "tmed","prec","tmin","horatmin","tmax","horatmax",
                                              "dir","velmedia","racha","horaracha","sol","presMax","horaPresMax",
                                              "presMin","horaPresMin","hrMedia","hrMax","horaHrMax","hrMin","horaHrMin",
                                              "fecha_dt","year","month")


    # ---- 5. Join con estaciones ----
    df_filtrado = landing_filtered.join(df_estaciones_select.withColumnRenamed("id", "id_estacion"),
                                       landing_filtered["indicativo"] == F.col("id_estacion"),
                                         "left").drop("id_estacion") \
        .select(
            F.col("fecha_dt").alias("fecha"),
            F.col("indicativo").alias("idema"), 
            F.col("nombre").alias("nombreEstacion"),
            F.col("municipio").alias("municipio"),
            F.col("provincia").alias("provincia"),
            F.col("altitud").alias("altitud"),
            F.col("tmed").alias("temperaturaMedia"), 
            F.col("prec").alias("precipitacion"), 
            F.col("tmin").alias("temperaturaMinima"), 
            F.col("horatmin").alias("horaTemperaturaMinima"), 
            F.col("tmax").alias("temperaturaMaxima"), 
            F.col("horatmax").alias("horaTemperaturaMaxima"), 
            F.col("dir").alias("direccionViento"), 
            F.col("velmedia").alias("velocidadMediaViento"),
            F.col("racha").alias("racha"),
            F.col("horaracha").alias("horaRacha"),
            F.col("sol").alias("radiacionSolar"),
            F.col("presMax").alias("presionMaxima"),
            F.col("horaPresMax").alias("horaPresionMaxima"),
            F.col("presMin").alias("presionMinima"),
            F.col("horaPresMin").alias("horaPresionMinima"),
            F.col("hrMedia").alias("humedaRelativaMedia"),
            F.col("hrMax").alias("humedadRelativaMaxima"),
            F.col("horaHrMax").alias("horaHumedadRelativaMaxima"),
            F.col("hrMin").alias("humedadRelativaMinima"),
            F.col("horaHrMin").alias("horaHumedadRelativaMinima"),
            F.col("year").alias("year"),
            F.col("month").alias("month"))

    # ---- 6. Limpieza básica ---- 
    print("Reemplazando comas a puntos en columnas numéricas...")
    df_clean = replace_commas_with_dots(df_filtrado, ["temperaturaMedia", "precipitacion", "temperaturaMinima",
                                                       "temperaturaMaxima",
                                                       "direccionViento", "vevelocidadMediaViento", "racha", 
                                                       "radiacionSolar", "presionMaxima", "presionMinima", 
                                                       "humedaRelativaMedia", "humedadRelativaMaxima","humedadRelativaMinima"])
    #print("Mostrando 10 registros de CLEAN:")
    #df_clean.show(10, truncate=False)


    # Escribir la tabla limpia en Iceberg
    print(f"Guardando datos limpios en Iceberg: {db_trusted}.{tbl_trusted}")
    utils.overwrite_iceberg_table(spark, df_clean, db_name=db_trusted, table_name=tbl_trusted)
    #mostrar donde se guado la tabla
    print(f"Datos guardados en spark_catalog.{db_trusted}.{tbl_trusted}")

    #mostrar las 1 fila
    print("Mostrando 1 fila de TRUSTED:")
    df_trusted= utils.read_iceberg_table(spark,db_name=db_trusted,table_name=tbl_trusted)
    df_trusted.show(1, truncate=False)

    #cantidad de registros trusted
    print(f"Cantidad de registros en TRUSTED: {df_trusted.count()}")

    spark.stop()
    # print(" Proceso completado")