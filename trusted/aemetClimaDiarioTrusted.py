from pyspark.sql.functions import upper, col, regexp_replace, lit, coalesce, when
from pyspark.sql.types import *
import utilsJoaquim_airflow as utils
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


# --------------------------
# Función para reemplazar comas por puntos en columnas string numéricas
# --------------------------
def replace_commas_with_dots(df, columns):
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, regexp_replace(col(c), ",", ".").cast(DoubleType()))
    return df


def imputeStringColumns(df: DataFrame, replacement_value: str = "Desconocido"):
    string_cols = [
        "fecha", "idema", "nombreEstacion", "municipio", "provincia",
        "horaTemperaturaMinima", "horaTemperaturaMaxima", "horaRacha",
        "horaPresionMaxima", "horaPresionMinima",
        "horaHumedadRelativaMaxima", "horaHumedadRelativaMinima"
    ]
    if not string_cols:
        print("No se encontraron columnas StringType para imputar.")
        return df

    columns_to_select = []
    for column_name in df.columns:
        if column_name in string_cols:
            columns_to_select.append(
                when(col(column_name).isNull() | (col(column_name) == ""), lit(replacement_value))
                .otherwise(col(column_name))
                .alias(column_name)
            )
        else:
            columns_to_select.append(col(column_name))

    print(f"Imputando nulls y strings vacíos con '{replacement_value}'...")
    return df.select(columns_to_select)


def imputeNumericColumns(df: DataFrame, constant_value: float = 999.0, columns_to_impute: list = None) -> DataFrame:
    numeric_spark_types = (IntegerType, LongType, FloatType, DoubleType, DecimalType)
    df_numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, numeric_spark_types)]

    conceptual_numeric = [
        "altitud", "temperaturaMedia", "precipitacion", "temperaturaMinima",
        "temperaturaMaxima", "direccionViento", "velocidadMediaViento", "racha",
        "radiacionSolar", "presionMaxima", "presionMinima",
        "humedadRelativaMedia", "humedadRelativaMaxima", "humedadRelativaMinima"
    ]

    target_cols = list(set(df_numeric_cols) & set(conceptual_numeric))
    if not target_cols:
        print("No se encontraron columnas numéricas para imputar.")
        return df

    columns_to_select = []
    for column_name in df.columns:
        if column_name in target_cols:
            columns_to_select.append(coalesce(col(column_name), lit(constant_value)).alias(column_name))
        else:
            columns_to_select.append(col(column_name))

    print(f"Imputando NULLs en columnas numéricas con valor {constant_value}...")
    return df.select(columns_to_select)


# --------------------------
# MAIN callable for Airflow
# --------------------------
def process_aemet_clima_diario_selected(**kwargs):
    """ETL process for AEMET Clima Diario Trusted zone"""

    spark = utils.create_context()
    spark.sparkContext.setLogLevel("ERROR")

    # Landing / Trusted tables
    landing_db = "landing"
    tbl_landing = "aemetClimaDiario"
    db_trusted = "trusted"
    tbl_trusted = "aemetClimaDiarioTrusted"

    print(f"→ Reading table spark_catalog.{landing_db}.{tbl_landing}")
    df_landing = utils.read_iceberg_table(spark, db_name=landing_db, table_name=tbl_landing)
    print(f"Landing row count: {df_landing.count()}")

    # Stations of interest
    data_estaciones_objetivo = [
        ("8036Y", "Benidorm", "Benidorm", "Alacant/Alicante", 70, "383306N", "000800W"),
        ("0201D", "Barcelona", "Barcelona", "Barcelona", 6, "412326N", "021200E"),
        ("0201X", "Barcelona, Museo Marítimo", "Barcelona", "Barcelona", 5, "412230N", "021026E"),
        ("B278", "Palma de Mallorca, Aeropuerto", "Palma de Mallorca", "Illes Balears", 8, "393339N", "024412W"),
        ("3129", "Madrid Aeropuerto", "Madrid", "Madrid", 609, "402800N", "033320W"),
        ("5790Y", "Sevilla, Tablada", "Sevilla", "Sevilla", 9, "372151N", "060021W"),
        ("8416Y", "Valencia", "Valencia", "Comunitat Valenciana", 11, "392850N", "002159W"),
    ]

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("nombre", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("provincia", StringType(), True),
        StructField("altura", IntegerType(), True),
        StructField("latitud", StringType(), True),
        StructField("longitud", StringType(), True),
    ])

    df_estaciones = spark.createDataFrame(data_estaciones_objetivo, schema)
    id_list = [row.id for row in df_estaciones.select("id").collect()]
    landing_filtered = df_landing.filter(F.col("indicativo").isin(id_list))

    # Select and clean columns
    landing_filtered = landing_filtered.select(
        "indicativo", "nombre", "altitud", "tmed", "prec", "tmin", "horatmin", "tmax", "horatmax",
        "dir", "velmedia", "racha", "horaracha", "sol", "presMax", "horaPresMax", "presMin",
        "horaPresMin", "hrMedia", "hrMax", "horaHrMax", "hrMin", "horaHrMin",
        "fecha_dt", "year", "month"
    )

    # Join with station info
    df_filtrado = landing_filtered.join(
        df_estaciones.withColumnRenamed("id", "id_estacion"),
        landing_filtered["indicativo"] == F.col("id_estacion"),
        "left"
    ).drop("id_estacion").select(
        F.col("fecha_dt").alias("fecha"),
        F.col("indicativo").alias("idema"),
        F.col("nombre").alias("nombreEstacion"),
        F.col("municipio"),
        F.col("provincia"),
        F.col("altitud"),
        F.col("tmed").alias("temperaturaMedia"),
        F.col("prec").alias("precipitacion"),
        F.col("tmin").alias("temperaturaMinima"),
        F.col("horatmin").alias("horaTemperaturaMinima"),
        F.col("tmax").alias("temperaturaMaxima"),
        F.col("horatmax").alias("horaTemperaturaMaxima"),
        F.col("dir").alias("direccionViento"),
        F.col("velmedia").alias("velocidadMediaViento"),
        F.col("racha"),
        F.col("horaracha").alias("horaRacha"),
        F.col("sol").alias("radiacionSolar"),
        F.col("presMax").alias("presionMaxima"),
        F.col("horaPresMax").alias("horaPresionMaxima"),
        F.col("presMin").alias("presionMinima"),
        F.col("horaPresMin").alias("horaPresionMinima"),
        F.col("hrMedia").alias("humedadRelativaMedia"),
        F.col("hrMax").alias("humedadRelativaMaxima"),
        F.col("horaHrMax").alias("horaHumedadRelativaMaxima"),
        F.col("hrMin").alias("humedadRelativaMinima"),
        F.col("horaHrMin").alias("horaHumedadRelativaMinima"),
        F.col("year"), F.col("month")
    )

    # Clean and impute
    print("Cleaning numeric and string nulls...")
    df_clean = replace_commas_with_dots(df_filtrado, [
        "temperaturaMedia", "precipitacion", "temperaturaMinima", "temperaturaMaxima",
        "direccionViento", "velocidadMediaViento", "racha", "radiacionSolar",
        "presionMaxima", "presionMinima", "humedadRelativaMedia",
        "humedadRelativaMaxima", "humedadRelativaMinima"
    ])

    df_imputed = (df_clean
                  .transform(imputeNumericColumns, constant_value=-9999)
                  .transform(imputeStringColumns, replacement_value="Desconocido"))

    # Write to Iceberg
    print(f"Writing cleaned data to spark_catalog.{db_trusted}.{tbl_trusted}")
    utils.overwrite_iceberg_table(spark, df_imputed, db_name=db_trusted, table_name=tbl_trusted)

    print("✅ Trusted load complete.")
    spark.stop()

if __name__ == "__main__":
	# Local quick-run for development / debugging
	process_aemet_clima_diario_selected(show_rows=10)