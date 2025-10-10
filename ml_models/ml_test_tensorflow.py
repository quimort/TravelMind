from pyspark.sql import SparkSession,DataFrame
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

def load_exploitation_table(spark: SparkSession,db_name:str, table_name: str) -> DataFrame:
    df = utils.read_iceberg_table(
        spark=spark,
        db_name=db_name,
        table_name=table_name
    )
    return df

if __name__ == "__main__":
    statr_experiment()
    spark = start_spark()

    df_joined = load_exploitation_table(
        spark=spark,
        db_name="exploitation",
        table_name="travelmind_features"
    )
    print(f"    Loaded records: {df_joined.count()}")

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
        "plazas_estimadas", "apt_personal_empleado","apt_availability_score_lag1",
        
        # Tráfico
        "trafico_imd_total_lag1",
        
        # Ocio
        "ocio_total_entradas", "ocio_gasto_total", "ocio_precio_medio_entrada",
        
        # Calidad del aire
        "aire_pct_buena_lag1", "aire_pct_aceptable", "aire_pct_mala",
        
        # Clima
        "temp_media_mes", "temp_min_media_mes_lag1", "temp_max_media_mes_lag1",
        "precipitacion_total_mes", "dias_lluvia_mes_lag1",
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
    