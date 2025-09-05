from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
import utils as utils
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from xgboost.spark import SparkXGBClassifier
from pyspark.ml import Pipeline
from builtins import min as python_min
import mlflow
import mlflow.spark


def statr_experiment():
    mlflow.set_experiment("visitas-ciudad-model")

def start_spark():
    spark = utils.create_context()
    return spark

def create_apartment_features(df_apartments):
    """Create apartment-based features."""
    print("  Creating apartment features...")
    
    df_apartment_features = df_apartments.groupBy(
        col("AÑO"),
        col("MES"),
        col("PROVINCIA")
    ).agg(
        sum("VIAJEROS").alias("apt_viajeros"),
        sum("PERNOCTACIONES").alias("apt_pernoctaciones"),
        avg("ESTANCIA_MEDIA").alias("apt_estancia_media"),
        avg("GRADO_OCUPA_PLAZAS").alias("avg_ocupa_plazas"),
        avg("GRADO_OCUPA_APART").alias("avg_ocupa_apart"),
        avg("GRADO_OCUPA_APART_FIN_SEMANA").alias("avg_ocupa_apart_weekend"),
        sum("APARTAMENTOS_ESTIMADOS").alias("apt_estimados"),
        sum("PLAZAS_ESTIMADAS").alias("plazas_estimadas"),
        sum("PERSONAL_EMPLEADO").alias("apt_personal_empleado")
    ).withColumn(
        # Apartment availability score (basado en ocupación de plazas)
        "apt_availability_score",
        100 - col("avg_ocupa_plazas")
    )
    
    return df_apartment_features

def create_leisure_features(df_leisure):
    """Create leisure-based features from actividades_ocio table."""
    print("  Creating leisure features...")

    df_leisure_features = df_leisure.groupBy(
        col("AÑO"),
        col("MES"),
        col("PROVINCIA")
    ).agg(
        sum("ENTRADAS").alias("ocio_total_entradas"),
        sum("VISITAS_PAGINAS").alias("ocio_total_visitas_paginas"),
        sum("GASTO_TOTAL").alias("ocio_gasto_total"),
        avg("PRECIO_MEDIO_ENTRADA").alias("ocio_precio_medio_entrada"),
        sum("TRANSACCIONES").alias("ocio_total_transacciones")
    ).withColumn(
        # Engagement score: visitas / transacciones (más alto = más interés online por compra)
        "ocio_engagement_score",
        (col("ocio_total_visitas_paginas") / (col("ocio_total_transacciones") + 1))
    ).withColumn(
        # Gasto medio por entrada
        "ocio_gasto_medio_por_entrada",
        (col("ocio_gasto_total") / (col("ocio_total_entradas") + 1))
    )

    return df_leisure_features

def create_air_quality_features(df_air):
    """Create air quality features from calidad_aire table."""
    print("  Creating air quality features...")

    df_air_features = df_air.groupBy(
        col("AÑO"),
        col("MES"),
        col("PROVINCIA")
    ).agg(
        # Porcentaje medio del mes de calidad de aire buena
        avg(when(col("CALIDAD_AIRE") == "Buena", col("PORCENTAJE_CALIDAD_AIRE"))).alias("aire_pct_buena"),
        # Porcentaje medio del mes de calidad de aire aceptable
        avg(when(col("CALIDAD_AIRE") == "Aceptable", col("PORCENTAJE_CALIDAD_AIRE"))).alias("aire_pct_aceptable"),
        # Porcentaje medio del mes de calidad de aire mala
        avg(when(col("CALIDAD_AIRE") == "Mala", col("PORCENTAJE_CALIDAD_AIRE"))).alias("aire_pct_mala"),
        # Número de estaciones monitorizadas
        countDistinct("ESTACION").alias("aire_num_estaciones")
    ).withColumn(
        # Índice simplificado: pondera calidad (buena=2, aceptable=1, mala=0)
        "aire_quality_index",
        (
            col("aire_pct_buena") * 2 +
            col("aire_pct_aceptable") * 1 +
            col("aire_pct_mala") * 0
        ) / (col("aire_pct_buena") + col("aire_pct_aceptable") + col("aire_pct_mala") + 1e-6)
    )

    return df_air_features

def create_trafico_features(df_trafico):
    """Create traffic features from trafico_semana table."""
    print("  Creating traffic features...")

    df_trafico_features = df_trafico.groupBy(
        col("AÑO"),
        col("MES"),
        col("PROVINCIA")
    ).agg(
        # Intensidad media de vehículos ligeros en el mes
        avg("IMD_VEHICULO_LIGERO").alias("trafico_imd_ligeros"),
        # Intensidad media de vehículos pesados en el mes
        avg("IMD_VEHICULO_PESADO").alias("trafico_imd_pesados"),
        # Intensidad media total
        avg("IMD_VEHICULO_TOTAL").alias("trafico_imd_total"),
        # Total mensual de vehículos (ligeros + pesados)
        sum("IMD_VEHICULO_TOTAL").alias("trafico_total_mes"),
        # Número de estaciones activas
        countDistinct("ESTACION").alias("trafico_num_estaciones")
    ).withColumn(
        # Ratio de pesados sobre total (indicador económico-logístico)
        "trafico_pct_pesados",
        col("trafico_imd_pesados") / (col("trafico_imd_total") + 1e-6)
    )

    return df_trafico_features


if __name__ == "__main__":
    statr_experiment()
    spark = start_spark()
    print("=== Loading source tables ===")

    # ------------------------------
    # Hoteles
    #print("  Loading hotel occupancy data...")
    #df_hotels = utils.read_iceberg_table(
    #    spark=spark, 
    #    db_name="exploitation", 
    #    table_name="f_ocupacion_barcelona"
    #)
    #print(f"    Hotel records: {df_hotels.count()}")
    print("  Loading apartamentos data...")
    df_apartamentos = utils.read_iceberg_table(
        spark=spark,
        db_name="trusted",
        table_name="apartamentos_ocupacion_selected"
    )
    print(f"    Apartamentos records: {df_apartamentos.count()}")

    print("  Loading actividades ocio data...")
    df_ocio = utils.read_iceberg_table(
        spark=spark,
        db_name="trusted",
        table_name="actividades_Ocio_selected"
    )
    print(f"    Ocio records: {df_ocio.count()}")

    print("  Loading calidad aire data...")
    df_calidad = utils.read_iceberg_table(
        spark=spark,
        db_name="trusted",
        table_name="calidad_aire_selected"
    )
    print(f"    Calidad aire records: {df_calidad.count()}")

    print("  Loading trafico semanal data...")
    df_trafico = utils.read_iceberg_table(
        spark=spark,
        db_name="trusted",
        table_name="trafico_semana_selected"
    )
    print(f"    Trafico records: {df_trafico.count()}")

    #2. Generar features a partir de df_hotels
    print("=== Generating features ===")

    # Llamar a las funciones de features
    df_apartamentos_features = create_apartment_features(df_apartamentos)
    df_ocio_features         = create_leisure_features(df_ocio)
    df_calidad_features      = create_air_quality_features(df_calidad)
    df_trafico_features      = create_trafico_features(df_trafico)

    print("  Joining datasets...")

    # Join de los datasets
    join_cols = ["PROVINCIA", "AÑO", "MES"]
    df_joined = (
        df_trafico_features.alias("t") 
        .join(df_ocio_features.alias("o"), join_cols, "left") 
        .join(df_calidad_features.alias("c"), join_cols, "left") 
        .join(df_apartamentos_features.alias("h"), join_cols, "left")
    )
    print(f"    Joined records: {df_joined.count()}")
    df_joined.printSchema()
    df_joined.show(1,truncate=False)
    # Manejo de