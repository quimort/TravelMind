import utils as utils
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, DecimalType, FloatType, DoubleType, IntegerType, BooleanType, MapType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, lit, when

#Crear una función para imputar nulos en las columnas numéricas de un Spark DataFrame
def imputeNumericColumns(
    df: DataFrame,
    constant_value: float = 999.0,
    columns_to_impute: list = None
) -> DataFrame:
    """
    Imputa valores NULL en columnas numéricas (IntegerType, FloatType, LongType, DoubleType, DecimalType) 
    de un DataFrame de Spark con un valor constante especificado.

    Esta función opera en columnas que *ya* son de tipo numérico.
    Si sus valores "vacíos" se representan actualmente como cadenas vacías ("") 
    u otro texto no numérico en una columna StringType, primero debe convertirlos a 
    valores NULL y convertir la columna a un tipo numérico mediante un paso de preprocesamiento.

    Args:
        df (DataFrame): El Spark DataFrame.
        constant_value (float): El valor constante para imputación. Por defecto será 999.0.
        columns_to_impute (list, optional): Una lista de nombres de columnas numéricas específicas para imputar.
                                            Si no hay ninguno, se imputarán todas las columnas numéricas.

    Returns:
        DataFrame: Un nuevo Spark DataFrame con NULL imputados en columnas numéricas.

    """
    if not isinstance(df, DataFrame):
        raise TypeError("Entrada 'df' debe ser un Spark DataFrame.")
    if not isinstance(constant_value, (int, float)):
        raise TypeError("Entrada'constant_value' debe ser un tipo numeric (int or float).")

    # Define the Spark numeric types this function will target
    numeric_spark_types = (IntegerType, LongType, FloatType, DoubleType, DecimalType)

    # Get all column names from the DataFrame's SCHEMA that are actually numeric types
    df_actual_numeric_cols_from_schema = [f.name for f in df.schema.fields if isinstance(f.dataType, numeric_spark_types)]

    # Your predefined lists for AEMET data
    all_aemet_cols_conceptual = ['prob_precipitacion_00_24','cota_nieve_prov_00_24','estado_cielo_00_24_descripcion','estado_cielo_00_24_code','viento_direccion_00_24','viento_velocidad_00_24','racha_max_00_24','temperatura_maxima','temperatura_minima','sens_termica_maxima','sens_termica_minima','humedad_relativa_maxima','humedad_relativa_minima','uv_max']
    string_aemet_cols_conceptual = ['estado_cielo_00_24_descripcion', 'viento_direccion_00_24']

    # Determine the conceptual numeric columns based on your AEMET lists
    conceptual_numeric_aemet_cols = list(set(all_aemet_cols_conceptual) - set(string_aemet_cols_conceptual))

    # The final set of numeric columns to consider for imputation are those that are:
    # 1. Actually numeric in the DataFrame's schema.
    # 2. Present in your conceptual list of AEMET numeric columns.
    # This ensures robustness.
    all_numeric_cols_to_target = list(set(df_actual_numeric_cols_from_schema) & set(conceptual_numeric_aemet_cols))


    if columns_to_impute:
        # If specific columns are requested, filter them against the actual numeric target list
        actual_columns_to_impute = [c for c in columns_to_impute if c in all_numeric_cols_to_target]
        if len(actual_columns_to_impute) != len(columns_to_impute):
            missing_or_non_target = set(columns_to_impute) - set(actual_columns_to_impute)
            print(f"Warning: Algunas columnas específicas ({missing_or_non_target}) no son target o no existen en el schema del DataFrame schema. Estas serán ignoradas.")
        numeric_cols_for_imputation = actual_columns_to_impute
    else:
        # If no specific columns, use all identified numeric target columns
        numeric_cols_for_imputation = all_numeric_cols_to_target

    if not numeric_cols_for_imputation:
        print("No se encontraron ni seleccionaron columnas numéricas para la imputación según los criterios. No se realizaron cambios.")
        return df

    print(f"Imputando NULLs en columans numéricas con valor constante: '{constant_value}'...")
    print(f"Columns affectadas: {', '.join(numeric_cols_for_imputation)}")

    columns_to_select = []
    for column_name in df.columns:
        if column_name in numeric_cols_for_imputation:
            # coalesce replaces NULLs. For numerical types, NULL is the only "empty" value.
            columns_to_select.append(
                coalesce(col(column_name), lit(constant_value)).alias(column_name)
            )
        else:
            columns_to_select.append(col(column_name))

    return df.select(columns_to_select)

#Crear una función para imputar nulos en las columnas de tipo StringType Spark DataFrame
def imputeStringColumns(df: DataFrame, replacement_value: str = "Desconocido"):
    """
    Imputa valores nulos en columnas de tipo StringType con un valor por defecto.
    """
    string_cols=['estado_cielo_00_24_descripcion', 'viento_direccion_00_24', 'cota_nieve_prov_00_24', 'estado_cielo_00_24_code', 'racha_max_00_24']
    #string_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    #menos columnas específicas que no queremos imputar
    #string_cols = [col for col in string_cols if col not in ['id', 'version']]  # Exclude 'id' and 'version' columns if they are StringType
    # Exclude columns that are not StringType or that we don't want to impute   
    #string_cols = [col for col in string_cols if col not in ['municipio_codigo_aemet', 'nombre_municipio_ine', 'fecha_descarga_utc', 'prediccion_fecha']] 
    
    if not string_cols:
        print("No se encontraron columns StringType para imputation.")
        return df

    # Create a list of all columns, applying the imputation logic for StringType columns
    columns_to_select = []
    for column_name in df.columns:
        if column_name in string_cols:
            columns_to_select.append(
                # Check if NULL OR if it's an empty string
                when(col(column_name).isNull() | (col(column_name) == ""), lit(replacement_value))
                .otherwise(col(column_name))
                .alias(column_name)
            )
        else:
            columns_to_select.append(col(column_name))
            
    print(f"Imputando nulls y strings vacios es columnas con '{replacement_value}'...")
    print(f"Columnas afectadas: {', '.join(string_cols)}")

    return df.select(columns_to_select)

#crear context spark
spark = utils.create_context()

#nombre de la base de datos y tabla raw
landing_db = "landing_db"
tbl_landing = "aemet_prediccion_diaria"
#Leemos los datos de la tabla iceberg
print(f" Leyendo tabla RAW: spark_catalog.{landing_db}.{tbl_landing}")
df_raw_aemet= utils.read_iceberg_table(spark,db_name=landing_db,table_name=tbl_landing)
#df_raw_aemet = spark.read.format("iceberg").load(f"spark_catalog.{landing_db}.{tbl_landing}")

# Define the schema for the nested JSON string in 'raw_aemet_data_json_str'
aemet_data_schema = ArrayType(StructType([
    StructField("origen", StructType([
        StructField("productor", StringType(), True),
        StructField("web", StringType(), True),
        StructField("enlace", StringType(), True),
        StructField("language", StringType(), True),
        StructField("copyright", StringType(), True),
        StructField("notaLegal", StringType(), True)
    ]), True),
    StructField("elaborado", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("provincia", StringType(), True),
    StructField("prediccion", StructType([
        StructField("dia", ArrayType(StructType([
            StructField("probPrecipitacion", ArrayType(StructType([
                StructField("value", IntegerType(), True),
                StructField("periodo", StringType(), True)
            ])), True),
            StructField("cotaNieveProv", ArrayType(StructType([
                StructField("value", IntegerType(), True),
                StructField("periodo", StringType(), True)
            ])), True),
            StructField("estadoCielo", ArrayType(StructType([
                StructField("value", StringType(), True),
                StructField("periodo", StringType(), True),
                StructField("descripcion", StringType(), True)
            ])), True),
            StructField("viento", ArrayType(StructType([
                StructField("direccion", StringType(), True),
                StructField("velocidad", IntegerType(), True),
                StructField("periodo", StringType(), True)
            ])), True),
            StructField("rachaMax", ArrayType(StructType([
                StructField("value", IntegerType(), True),
                StructField("periodo", StringType(), True)
            ])), True),
            StructField("temperatura", StructType([
                StructField("maxima", IntegerType(), True),
                StructField("minima", IntegerType(), True),
                StructField("dato", ArrayType(StructType([
                    StructField("value", IntegerType(), True),
                    StructField("hora", IntegerType(), True)
                ])), True)
            ]), True),
            StructField("sensTermica", StructType([
                StructField("maxima", IntegerType(), True),
                StructField("minima", IntegerType(), True),
                StructField("dato", ArrayType(StructType([
                    StructField("value", IntegerType(), True),
                    StructField("hora", IntegerType(), True)
                ])), True)
            ]), True),
            StructField("humedadRelativa", StructType([
                StructField("maxima", IntegerType(), True),
                StructField("minima", IntegerType(), True),
                StructField("dato", ArrayType(StructType([
                    StructField("value", IntegerType(), True),
                    StructField("hora", IntegerType(), True)
                ])), True)
            ]), True),
            StructField("uvMax", IntegerType(), True),
            StructField("fecha", StringType(), True)
        ])), True)
    ]), True),
    StructField("id", LongType(), True),
    StructField("version", DoubleType(), True)
]))

# Apply the from_json function to parse the raw_aemet_data_json_str column
df_parsed = df_raw_aemet.withColumn(
    "parsed_aemet_data",
    F.from_json(F.col("raw_aemet_data_json_str"), aemet_data_schema)
)

# Explode the 'dia' array to get one row per day for each municipality
df_exploded_days = df_parsed.withColumn(
    "day_data",
    F.explode_outer(F.col("parsed_aemet_data").getItem(0).getField("prediccion").getField("dia"))
)

# Function to filter an array of structs for the "00-24" period and get its value
def get_period_value(col_name, value_field="value"):
    return F.expr(f"""
        FILTER({col_name}, element -> element.periodo = '00-24')[0].{value_field}
    """)

# Select and extract the desired data, focusing on the '00-24' period for relevant arrays
df_daily_summary = df_exploded_days.select(
    F.col("municipio_codigo_aemet"),
    F.col("nombre_municipio_ine"),
    F.col("fecha_descarga_utc"),
    F.col("day_data.fecha").alias("prediccion_fecha"),

    # Probabilidad de precipitación 00-24
    get_period_value("day_data.probPrecipitacion", "value").alias("probPrecipitacion_00_24"),

    # Cota de nieve provincial 00-24
    get_period_value("day_data.cotaNieveProv", "value").alias("cotaNieveProv_00_24"),

    # Estado del cielo 00-24
    get_period_value("day_data.estadoCielo", "descripcion").alias("estadoCieloDescripcion_00_24"),
    get_period_value("day_data.estadoCielo", "value").alias("estadoCieloCode_00_24"),

    # Viento 00-24
    get_period_value("day_data.viento", "direccion").alias("vientoDireccion_00_24"),
    get_period_value("day_data.viento", "velocidad").alias("vientoVelocidad_00_24"),

    # Racha máxima 00-24
    get_period_value("day_data.rachaMax", "value").alias("rachaMaxima_00_24"),

    # Temperatura
    F.col("day_data.temperatura.maxima").alias("temperaturaMaxima"),
    F.col("day_data.temperatura.minima").alias("temperaturaMinima"),

    # Sensación térmica
    F.col("day_data.sensTermica.maxima").alias("sensTermicaMaxima"),
    F.col("day_data.sensTermica.minima").alias("sensTermicaMinima"),

    # Humedad Relativa
    F.col("day_data.humedadRelativa.maxima").alias("humedadRelativaMaxima"),
    F.col("day_data.humedadRelativa.minima").alias("humedadRelativaMinima"),

    # UV Max
    F.col("day_data.uvMax").alias("uv_max")
)

# Impute missing values in numeric and string columns
# Note: This is a simplified example. In practice, you might want to handle different types of missing values differently.
# Impute numeric columns with a constant value of -9999 
df_imputed = (df_daily_summary
              .transform(imputeNumericColumns, constant_value=-9999)
              .transform(imputeStringColumns, replacement_value="Desconocido")
             )

db_name = "trusted_db"
table_name = "prediccion_clima_diario"

utils.overwrite_iceberg_table(spark,df_imputed,db_name,table_name)

spark.stop()
