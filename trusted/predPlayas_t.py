import utils as utils
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, DecimalType, FloatType, DoubleType, IntegerType, BooleanType, MapType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

#crear context spark
spark = utils.create_context()

#nombre de la base de datos y tabla raw
landing_db = "landing_db"
tbl_landing = "aemet_prediccion_playa"
#Leemos los datos de la tabla iceberg
print(f" \nLeyendo tabla RAW: spark_catalog.{landing_db}.{tbl_landing}")
df_raw_aemet= utils.read_iceberg_table(spark,db_name=landing_db,table_name=tbl_landing)

# Define the schema for the nested JSON string in 'raw_aemet_data_json_str'column
schema = ArrayType(StructType([
    StructField("origen", StructType([
        StructField("productor", StringType(), True),
        StructField("web", StringType(), True),
        StructField("language", StringType(), True),
        StructField("copyright", StringType(), True),
        StructField("notaLegal", StringType(), True),
    ]), True),
    StructField("elaborado", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("localidad", IntegerType(), True),
    StructField("prediccion", StructType([
        StructField("dia", ArrayType(StructType([
            StructField("estadoCielo", StructType([
                StructField("value", StringType(), True),
                StructField("f1", IntegerType(), True),
                StructField("descripcion1", StringType(), True),
                StructField("f2", IntegerType(), True),
                StructField("descripcion2", StringType(), True),
            ]), True),
            StructField("viento", StructType([
                StructField("value", StringType(), True),
                StructField("f1", IntegerType(), True),
                StructField("descripcion1", StringType(), True),
                StructField("f2", IntegerType(), True),
                StructField("descripcion2", StringType(), True),
            ]), True),
            StructField("oleaje", StructType([
                StructField("value", StringType(), True),
                StructField("f1", IntegerType(), True),
                StructField("descripcion1", StringType(), True),
                StructField("f2", IntegerType(), True),
                StructField("descripcion2", StringType(), True),
            ]), True),
            StructField("tMaxima", StructType([
                StructField("value", StringType(), True),
                StructField("valor1", IntegerType(), True),
            ]), True),
            StructField("sTermica", StructType([
                StructField("value", StringType(), True),
                StructField("valor1", IntegerType(), True),
                StructField("descripcion1", StringType(), True),
            ]), True),
            StructField("tAgua", StructType([
                StructField("value", StringType(), True),
                StructField("valor1", IntegerType(), True),
            ]), True),
            StructField("uvMax", StructType([
                StructField("value", StringType(), True),
                StructField("valor1", IntegerType(), True),
            ]), True),
            StructField("fecha", IntegerType(), True),
            StructField("tagua", StructType([
                StructField("value", StringType(), True),
                StructField("valor1", IntegerType(), True),
            ]), True),
            StructField("tmaxima", StructType([
                StructField("value", StringType(), True),
                StructField("valor1", IntegerType(), True),
            ]), True),
            StructField("stermica", StructType([
                StructField("value", StringType(), True),
                StructField("valor1", IntegerType(), True),
                StructField("descripcion1", StringType(), True),
            ]), True),
        ])), True),
    ]), True),
    StructField("id", IntegerType(), True),
]))
# Parse the nested JSON string in 'raw_aemet_data_json_str' column
df_parsed = df_raw_aemet.withColumn("parsed_aemet_data", F.from_json(F.col("raw_aemet_data_json_str"), schema))

# Explode the 'dia' array to get one row per day for each beach
df_exploded = df_parsed.withColumn(
    "day_data", 
    F.explode_outer(F.col("parsed_aemet_data").getItem(0).getField("prediccion").getField("dia")))


# Extraer variables de nivel superior
df_flat = df_exploded.select(
    F.col("playa_codigo_aemet").alias("id_playa"),

    #F.col("parsed_aemet_data").getItem(0).getField("id").alias("id_playa"),
    F.col("parsed_aemet_data").getItem(0).getField("nombre").alias("nombre_playa"),
    F.col("nombre_municipio").alias("nombre_municipio"),
    F.col("nombre_provincia").alias("nombre_provincia"),
    F.col("parsed_aemet_data").getItem(0).getField("localidad").alias("codigo_municipio"),
    F.col("day_data.fecha").alias("fecha"),

    # Estado del cielo
    F.col("day_data.estadoCielo.f1").alias("estadoCielo_f1"),
    F.col("day_data.estadoCielo.descripcion1").alias("estadoCielo_desc1"),
    F.col("day_data.estadoCielo.f2").alias("estadoCielo_f2"),
    F.col("day_data.estadoCielo.descripcion2").alias("estadoCielo_desc2"),

    # Viento
    F.col("day_data.viento.f1").alias("viento_f1"),
    F.col("day_data.viento.descripcion1").alias("viento_desc1"),
    F.col("day_data.viento.f2").alias("viento_f2"),
    F.col("day_data.viento.descripcion2").alias("viento_desc2"),

    # Oleaje
    F.col("day_data.oleaje.f1").alias("oleaje_f1"),
    F.col("day_data.oleaje.descripcion1").alias("oleaje_desc1"),
    F.col("day_data.oleaje.f2").alias("oleaje_f2"),
    F.col("day_data.oleaje.descripcion2").alias("oleaje_desc2"),

    # Temperatura máxima
    F.col("day_data").getField("tMaxima").getField("valor1").alias("tMaxima"),

    # Sensación térmica
    F.col("day_data").getField("sTermica").getField("valor1").alias("sTermica_valor1"),
    F.col("day_data").getField("sTermica").getField("descripcion1").alias("sTermica_desc1"),

    # Temperatura del agua
    F.col("day_data").getField("tAgua").getField("valor1").alias("tAgua_valor1"),
   

    # Índice UV máximo
    F.col("day_data.uvMax.valor1").alias("uvMax"),


    # Alias alternativos (duplicados en JSON)
    F.col("day_data").getField("tagua").getField("valor1").alias("tempAgua"),
    F.col("day_data").getField("tmaxima").getField("valor1").alias("tempMaxima"),
    F.col("day_data").getField("stermica").getField("valor1").alias("sensacTermica_valor1"),
    F.col("day_data").getField("stermica").getField("descripcion1").alias("sensacTermica_desc1")
 )

# Mostrar resultados
df_flat.show(truncate=False)

db_name = "trusted_db"
table_name = "prediccion_playa"

utils.overwrite_iceberg_table(spark,df_flat,db_name,table_name)
print(f"\n Datos guardados en tabla trusted: spark_catalog.{db_name}.{table_name}")

#mostrar cantidad de registros guardados
#print(f"\n Registros guardados: {utils.read_iceberg_table(spark,db_name= db_name,table_name=table_name).count()} registros")
spark.stop()