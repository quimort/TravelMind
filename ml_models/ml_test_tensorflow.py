from pyspark.sql import SparkSession,Row,DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
import utils as utils
from pyspark.sql.functions import *
import pandas as pd
import mlflow
import mlflow.sklearn
import xgboost as xgb
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import (
    accuracy_score, roc_auc_score, f1_score, precision_score, recall_score
)
from collections import Counter
from collections import Counter
from builtins import min as python_min
from datetime import date


def statr_experiment():
    mlflow.set_experiment("visitas-ciudad-model")

def start_spark():
    spark = utils.create_context()
    return spark
def convert_time_to_decimal(col_name):
    return (
        (split(col(col_name), ":").getItem(0).cast("double")) + 
        (split(col(col_name), ":").getItem(1).cast("double") / 60.0)
    )
def convert_decimal_to_time(col_name):
    hours = floor(col(col_name)).cast("int")
    minutes = round((col(col_name) - hours) * 60).cast("int")
    
    return (
        format_string("%02d:%02d", hours, minutes)
    )
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
        col("PROVINCIA"),
        col("PRODUCTO"),
        col("CATEGORIA")
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
        col("PROVINCIA"),
        col("DIA_SEMANA")
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
    ).withColumn(
        "dia_numero",
         when(col("DIA_SEMANA") == "Lunes", 1)
        .when(col("DIA_SEMANA") == "Martes", 2)
        .when(col("DIA_SEMANA") == "Miércoles", 3)
        .when(col("DIA_SEMANA") == "Jueves", 4)
        .when(col("DIA_SEMANA") == "Viernes", 5)
        .when(col("DIA_SEMANA") == "Sábado", 6)
        .when(col("DIA_SEMANA") == "Domingo", 7)
    )
    return df_trafico_features

def crate_clima_features(df_clima:DataFrame):

    df_clima = (
        df_clima
        .withColumn("horaTemperaturaMinima_dec", convert_time_to_decimal("horaTemperaturaMinima")) 
        .withColumn("horaTemperaturaMaxima_dec", convert_time_to_decimal("horaTemperaturaMaxima")) 
        .groupBy(
            col("month"),
            col("year"),
            col("provincia")
        ).agg(
            avg("temperaturaMedia").alias("temp_media_mes"),
    
            # Precipitaciones
            sum("precipitacion").alias("precipitacion_total_mes"),
            count(when(col("precipitacion") > 0, 1)).alias("dias_lluvia_mes"),
            
            # Temperatura mínima y máxima
            avg("temperaturaMinima").alias("temp_min_media_mes"),
            min("temperaturaMinima").alias("temp_min_abs_mes"),
            avg("temperaturaMaxima").alias("temp_max_media_mes"),
            max("temperaturaMaxima").alias("temp_max_abs_mes"),
            
            # Rangos térmicos
            (avg("temperaturaMaxima") - avg("temperaturaMinima")).alias("rango_termico_medio"),
            (max("temperaturaMaxima") - min("temperaturaMinima")).alias("rango_termico_extremo"),
            
            # Extra: número de días cálidos / fríos
            count(when(col("temperaturaMaxima") > 30, 1)).alias("dias_calidos"),
            count(when(col("temperaturaMinima") < 0, 1)).alias("dias_helada"),
            avg("horaTemperaturaMinima_dec").alias("hora_temp_min_media"),
            avg("horaTemperaturaMaxima_dec").alias("hora_temp_max_media")
        )
    )

    df_clima = (
        df_clima
        .withColumn("hora_temp_min_media", convert_decimal_to_time("hora_temp_min_media"))
        .withColumn("hora_temp_max_media", convert_decimal_to_time("hora_temp_max_media"))
        .select(
            col("year").alias("AÑO"),
            col("month").alias("MES"),
            col("provincia").alias("PROVINCIA"),
            col("temp_media_mes"),
            col("precipitacion_total_mes"),
            col("dias_lluvia_mes"),
            col("temp_min_media_mes"),
            col("temp_min_abs_mes"),
            col("temp_max_media_mes"),
            col("temp_max_abs_mes"),
            col("rango_termico_medio"),
            col("rango_termico_extremo"),
            col("dias_calidos"),
            col("dias_helada"),
            col("hora_temp_min_media"),
            col("hora_temp_max_media")
        )
    )
    return df_clima

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

    print("  Loading clima diario data...")
    df_clima_diario = utils.read_iceberg_table(
        spark=spark,
        db_name="trusted_db",
        table_name="aemetClimaDiarioTrusted"
    )
    print(f" clima diario records: {df_clima_diario.count()}")

    #2. Generar features a partir de df_hotels
    print("=== Generating features ===")

    # Llamar a las funciones de features
    df_apartamentos_features = create_apartment_features(df_apartamentos)
    df_ocio_features         = create_leisure_features(df_ocio)
    df_calidad_features      = create_air_quality_features(df_calidad)
    df_trafico_features      = create_trafico_features(df_trafico)
    df_clima_features        = crate_clima_features(df_clima_diario)

    print(f"apartamento features:{df_apartamentos_features.count()}")
    print(f"ocio features:{df_ocio_features.count()}")
    print(f"calidad aire features:{df_calidad_features.count()}")
    print(f"trafico features:{df_trafico_features.count()}")
    print(f"clima features:{df_clima_features.count()}")

    print("  Joining datasets...")

    # Join de los datasets
    join_cols = ["PROVINCIA", "AÑO", "MES"]
    df_joined = (
        df_trafico_features.alias("t") 
        .join(df_ocio_features.alias("o"), join_cols, "left") 
        .join(df_calidad_features.alias("c"), join_cols, "left") 
        .join(df_apartamentos_features.alias("h"), join_cols, "left")
        .join(df_clima_features.alias("cl"), join_cols, "left")
    )
    print(f"    Joined records: {df_joined.count()}")
    
    # 1. Calcular percentiles 33 y 66
    quantiles = df_joined.approxQuantile(
        ["trafico_imd_total", "aire_pct_buena", "apt_availability_score"],
        [0.33, 0.66],
        0.01
    )

    (traf_p33, traf_p66), (aire_p33, aire_p66), (apt_p33, apt_p66) = quantiles

    print("Umbrales tráfico:", traf_p33, traf_p66)
    print("Umbrales aire:", aire_p33, aire_p66)
    print("Umbrales alojamiento:", apt_p33, apt_p66)
    # Definir percentiles previamente calculados de temperatura y precipitación
    temp_p10 = 5   # ejemplo: 10ºC
    temp_p90 = 30  # ejemplo: 30ºC
    dias_lluvia = 10  # ejemplo: 10mm (percentil 66)
    # 2. Definir la label binaria
    df_labeled = df_joined.withColumn(
        "label",
        when(
            (col("trafico_imd_total") > traf_p66) |
            (col("aire_pct_buena") < aire_p33) |
            (col("apt_availability_score") < apt_p33) |
            (col("temp_min_media_mes") < temp_p10) |   
            (col("temp_max_media_mes") > temp_p90) |   
            (col("dias_lluvia_mes") > dias_lluvia),
            0  # Malo
        ).otherwise(1)  # Bueno
    )

    feature_cols = [
        # Turismo
        "apt_viajeros", "apt_pernoctaciones", "apt_estancia_media", "apt_estimados",
        "plazas_estimadas", "apt_personal_empleado","apt_availability_score",
        
        # Tráfico
        "trafico_imd_total",
        
        # Ocio
        "ocio_total_entradas", "ocio_gasto_total", "ocio_precio_medio_entrada",
        
        # Calidad del aire
        "aire_pct_buena", "aire_pct_aceptable", "aire_pct_mala",
        
        # Clima
        "temp_media_mes", "temp_min_media_mes", "temp_max_media_mes",
        "precipitacion_total_mes", "dias_lluvia_mes",
        "dias_calidos", "dias_helada"
    ]

    df_labeled = df_labeled.fillna(0, subset=feature_cols)
    df_pd = df_labeled.toPandas()

    
X = df_pd[feature_cols].values
y = df_pd["label"].astype(int).values

# Split 80/20 (como randomSplit de Spark)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# --- 3) Calcular ratio desbalance ---
counter = Counter(y_train)
num_pos = counter.get(1, 0)
num_neg = counter.get(0, 0)
ratio = num_neg / num_pos
print(f"Positivos: {num_pos}, Negativos: {num_neg}, Ratio (neg/pos): {ratio:.2f}")

# --- 4) Definir clasificador base ---
xgb_clf = xgb.XGBClassifier(
    objective="binary:logistic",
    eval_metric="auc",
    scale_pos_weight=ratio,
    random_state=42,
    n_jobs=-1
)

# --- 5) Grid de hiperparámetros ---
param_grid = {
    "max_depth": [3, 5, 7],
    "learning_rate": [0.1, 0.05, 0.01],
    "n_estimators": [100, 200],
    "subsample": [0.8, 1.0],
    "colsample_bytree": [0.8, 1.0],
    "min_child_weight": [1, 5],
    "gamma": [0, 1]
}

grid = GridSearchCV(
    estimator=xgb_clf,
    param_grid=param_grid,
    scoring="roc_auc",  
    cv=10,
    n_jobs=-1,
    verbose=1
)

# --- 6) MLflow logging ---
mlflow.set_experiment("travelmind_xgb_models")

with mlflow.start_run() as run:

    mlflow.log_param("cv_folds", 10)
    mlflow.log_param("scale_pos_weight", ratio)
    # Entrenar
    grid.fit(X_train, y_train)

    # Mejor modelo
    best_model = grid.best_estimator_
    print("Best params:", grid.best_params_)

    # Evaluar en test
    y_pred = best_model.predict(X_test)
    y_pred_prob = best_model.predict_proba(X_test)[:, 1]

    acc = accuracy_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_pred_prob)
    f1 = f1_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)

    print(f"Accuracy: {acc:.3f}")
    print(f"AUC: {auc:.3f}")
    print(f"F1: {f1:.3f}")
    print(f"Precision: {precision:.3f}")
    print(f"Recall: {recall:.3f}")

    # Log parámetros y métricas
    mlflow.log_params(grid.best_params_)
    mlflow.log_param("scale_pos_weight", ratio)
    mlflow.log_metrics({
        "accuracy": acc,
        "auc": auc,
        "f1": f1,
        "precision": precision,
        "recall": recall
    })

    # Loggear modelo
    mlflow.sklearn.log_model(
        sk_model=best_model,
        artifact_path="xgb_model",
        input_example=X_test[:5],
        signature=mlflow.models.infer_signature(X_test, y_test)
    )

    # Registrar en Model Registry
    model_uri = f"runs:/{run.info.run_id}/xgb_model"
    try:
        mlflow.register_model(model_uri=model_uri, name="travelmind_xgb_model")
        print("Modelo registrado en MLflow Model Registry.")
    except Exception as e:
        print("No se pudo registrar en el Model Registry:", e)

    print("Run ID:", run.info.run_id)
    