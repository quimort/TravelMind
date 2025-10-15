from pyspark.sql.functions import upper, col, regexp_replace, lit, coalesce, when
from pyspark.sql.types import *
import utils as utils
from pyspark.sql import functions as F
from pyspark.sql import DataFrame



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

#Crear una función para imputar nulos en las columnas de tipo StringType Spark DataFrame
def imputeStringColumns(df: DataFrame, replacement_value: str = "Desconocido"):
    """
    Imputa valores nulos en columnas de tipo StringType con un valor por defecto.
    """
    string_cols=['fecha', 'idema', 'nombreEstacion', 'municipio', 
                                    'provincia','horaTemperaturaMinima','horaTemperaturaMaxima',
                                      'horaRacha', 'horaPresionMaxima', 'horaPresionMinima', 
                                      'horaHumedadRelativaMaxima', 'horaHumedadRelativaMinima']
    
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
    all_aemet_cols_conceptual = ['altitud','temperaturaMedia','precipitacion','temperaturaMinima',
                                 'temperaturaMaxima','direccionViento','velocidadMediaViento',
                                 'racha','radiacionSolar','presionMaxima','presionMinima',
                                 'humedadRelativaMedia','humedadRelativaMaxima',
                                 'humedadRelativaMinima']
    string_aemet_cols_conceptual = ['fecha', 'idema', 'nombreEstacion', 'municipio', 
                                    'provincia','horaTemperaturaMinima','horaTemperaturaMaxima',
                                      'horaRacha', 'horaPresionMaxima', 'horaPresionMinima', 
                                      'horaHumedadRelativaMaxima', 'horaHumedadRelativaMinima']

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

    print(f"Imputando NULLs en columnas numéricas con valor constante: '{constant_value}'...")
    print(f"Las columnas numéricas imputadas en el DataSet son: {', '.join(numeric_cols_for_imputation)}")

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

if __name__ == "__main__":
    # 1. Crear sesión Spark
    spark = utils.create_context()

    # Tablas (ojo al catálogo)
    # Ajusta estos nombres según tu configuración
    db_trusted = "trusted"
    tbl_trusted = "aemet_clima_diario_trusted"

    #nombre de la base de datos y tabla raw
    landing_db = "landing"
    tbl_landing = "aemet_clima_diario"
    print(f" Leyendo tabla RAW: spark_catalog.{landing_db}.{tbl_landing}")
    df_landing= utils.read_iceberg_table(spark,db_name=landing_db,table_name=tbl_landing)

    #mostrar 1 filas de tabla landing
    #print("Mostrando 2 fila de LANDING:")
    #df_landing.show(2, truncate=False)
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
            F.col("year").alias("AÑO"),
            F.col("month").alias("MES"))

    # ---- 6. Limpieza básica ---- 
    #---- 6.1 Reemplazar comas por puntos en columnas numéricas ----
    # Primero imputar nulos en columnas numéricas para evitar errores en el reemplazo y casteo
    print("Reemplazando comas a puntos en columnas numéricas...")
    df_clean = replace_commas_with_dots(df_filtrado, ["temperaturaMedia", "precipitacion", "temperaturaMinima",
                                                       "temperaturaMaxima",
                                                       "direccionViento", "velocidadMediaViento", "racha", 
                                                       "radiacionSolar", "presionMaxima", "presionMinima", 
                                                       "humedadRelativaMedia", "humedadRelativaMaxima","humedadRelativaMinima"])
    #print("Mostrando 10 registros de CLEAN:")
    #df_clean.show(10, truncate=False)

    #---- 6.1 Imputar nulos en columnas StringType ----
    print("Iniciando Imputación de datos numericos y strings...")
    df_imputed = (df_clean
              #.transform(imputeNumericColumns, constant_value=-9999)
              .transform(imputeStringColumns, replacement_value="Desconocido")
             )
    # Escribir la tabla limpia en Iceberg
    print(f"Guardando datos limpios en Iceberg: {db_trusted}.{tbl_trusted}")
    utils.overwrite_iceberg_table(spark, df_imputed, db_name=db_trusted, table_name=tbl_trusted)
    # if spark.catalog.tableExists(dbName=db_trusted, tableName=tbl_trusted):
    #     spark.sql(f"DROP TABLE spark_catalog.{db_trusted}.{tbl_trusted}")
    #     df_imputed.writeTo(f"spark_catalog.{db_trusted}.{tbl_trusted}").using("iceberg").create()

    #mostrar donde se guado la tabla
    print(f"Datos guardados en spark_catalog.{db_trusted}.{tbl_trusted}")

    # #mostrar las 1 fila
    # print("Mostrando 1 fila de TRUSTED:")
    # df_trusted= utils.read_iceberg_table(spark,db_name=db_trusted,table_name=tbl_trusted)
    # df_trusted.show(10, truncate=False)

    # #cantidad de registros trusted
    # print(f"Cantidad de registros en TRUSTED: {df_trusted.count()}")

    spark.stop()
    # print(" Proceso completado")