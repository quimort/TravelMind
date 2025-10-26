#reentrenar modelo v1
# =======================================================
# Train, register XGBoost model and wrap as PyFunc
# =======================================================

import pandas as pd
import mlflow
import mlflow.sklearn
import mlflow.pyfunc
import xgboost as xgb
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import accuracy_score, roc_auc_score, f1_score, precision_score, recall_score
from collections import Counter
from datetime import datetime

mlflow.set_tracking_uri("file:///content/drive/MyDrive/TravelMind/mlruns")
#mlflow.set_experiment("travelmind_experiment")

# ===========================
# 1️⃣ Cargar features
# ===========================
df_base = pd.read_parquet("./exploitation/travelmind_features.parquet")
print(f"Registros cargados: {len(df_base)}")


# ===========================
# 2️⃣ Definir labels binarias
# ===========================
# Ejemplo: percentiles arbitrarios
aire_p33, aire_p66 = df_base["aire_pct_buena"].quantile([0.33, 0.66])
apt_p33, apt_p66 = df_base["apt_availability_score"].quantile([0.33, 0.66])
temp_p10, temp_p90 = 5, 30
dias_lluvia = 10

df_base["label"] = ((df_base["aire_pct_buena"] < aire_p33) |
               (df_base["apt_availability_score"] < apt_p33) |
               (df_base["temp_min_media_mes"] < temp_p10) |
               (df_base["temp_max_media_mes"] > temp_p90) |
               (df_base["dias_lluvia_mes"] > dias_lluvia)).astype(int)

# ===========================
# 3️⃣ Definir features
# ===========================
# Columnas/features que usarás
feature_cols = [
    # Turismo
    "apt_viajeros", "apt_pernoctaciones", "apt_estancia_media", "apt_estimados",
    "plazas_estimadas", "apt_personal_empleado","apt_availability_score_lag1",

    # Ocio
    "ocio_total_entradas", "ocio_gasto_total", "ocio_precio_medio_entrada",

    # Calidad del aire
    "aire_pct_buena_lag1", "aire_pct_aceptable", "aire_pct_mala",

    # Clima
    "temp_media_mes", "temp_min_media_mes_lag1", "temp_max_media_mes_lag1",
    "precipitacion_total_mes", "dias_lluvia_mes_lag1",
    "dias_calidos", "dias_helada",

    # Percepción
    "indice_percepcion_turistica_global", "indice_percepcion_seguridad",
    "indice_satisfaccion_productos_turisticos","indice_percepcion_climatica"
]

X = df_base[feature_cols].values
y = df_base["label"].values

# ===========================
# 4️⃣ Train/test split
# ===========================
# Split 80/20 (como randomSplit de Spark)
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# ===========================
# 5️⃣ Calcular ratio desbalance
# ===========================
counter = Counter(y_train)
num_pos = counter.get(1, 0)
num_neg = counter.get(0, 0)
ratio = num_neg / num_pos
print(f"Positivos: {num_pos}, Negativos: {num_neg}, Ratio (neg/pos): {ratio:.2f}")

# ===========================
# 6️⃣ Definir clasificador XGBoost
# ===========================
#Entrenar XGBoost con GridSearchCV
#Definir clasificador base ---
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

mlflow.set_experiment("travelmind_xgb_models")
# ===========================
# 7️⃣ Entrenar y registrar XGBoost
# ===========================
with mlflow.start_run() as run:
    grid.fit(X_train, y_train)
    best_model = grid.best_estimator_
    print("Best params:", grid.best_params_)

    # Evaluar
    y_pred = best_model.predict(X_test)
    y_pred_prob = best_model.predict_proba(X_test)[:,1]
    acc = accuracy_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_pred_prob)
    f1 = f1_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    print(f"Accuracy={acc:.3f}, AUC={auc:.3f}, F1={f1:.3f}, Precision={precision:.3f}, Recall={recall:0.3f}")

    # Log metrics y parámetros
    mlflow.log_params(grid.best_params_)
    mlflow.log_param("scale_pos_weight", ratio)
    mlflow.log_metrics({
        "accuracy": acc,
        "auc": auc,
        "f1": f1,
        "precision": precision,
        "recall": recall
    })

    # Log modelo
    mlflow.sklearn.log_model(
        sk_model=best_model,
        name="xgb_model",
        #input_example=X_test[:5],
        #signature=mlflow.models.infer_signature(X_test, y_test),
        registered_model_name="travelmind_xgb_model"
    )
    print("Modelo XGBoost registrado en MLflow")

# Guardar parquet en carpeta temporal para empaquetar
artifact_parquet = "./travelmind_features.parquet"
df_base.to_parquet(artifact_parquet)

# ===========================
# 8️⃣ Crear PyFunc completo que empaqueta parquet
# ===========================
class TravelMindPyFunc(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        # Cargar modelo desde MLflow Model Registry
        model_name = "travelmind_xgb_model"
        model_version = 1 # Cambia versión si hace falta
        model_uri = f"models:/{model_name}/{model_version}"

        self.model = mlflow.sklearn.load_model(model_uri)
        # Cargar tabla base
        self.df_base = pd.read_parquet(context.artifacts.get["travelmind_features"])

    def enrich_data(self, ciudad, fecha):
        dt = pd.to_datetime(fecha)
        month=dt.month, year = dt.year, day_number=dt.weekday()+1
        df_filtered = self.df_base[
            (self.df_base["PROVINCIA"]==ciudad) &
            (self.df_base["MES"]==month) &
            (self.df_base["dia_numero"]==day_number)
        ]
        feature_cols_local = self.df_base.columns.intersection(feature_cols)
        df_grouped = df_filtered.groupby(["PROVINCIA","MES","dia_numero"], as_index=False)[feature_cols_local].mean()
        df_grouped["AÑO"] = year
        return df_grouped[feature_cols_local]

    def predict(self, context, model_input: pd.DataFrame):
        results = []
        for _, row in model_input.iterrows():
            X_input = self.enrich_data(row["ciudad"], row["fecha"])
            if X_input.empty:
                results.append({"ciudad": row["ciudad"], "fecha": row["fecha"], "prediction": None, "probability": None})
                continue
            preds = self.model.predict(X_input)
            probs = self.model.predict_proba(X_input)[:,1]
            results.append({"ciudad": row["ciudad"], "fecha": row["fecha"], "prediction": int(preds[0]), "probability": float(probs[0])})
        return pd.DataFrame(results)

# ------------------------------------------
# 4️⃣ Guardar PyFunc con parquet incluido
# ------------------------------------------
with mlflow.start_run() as run:
    mlflow.pyfunc.log_model(
        name="travelmind_enriched_model",
        python_model=TravelMindPyFunc(),
        artifacts={"travelmind_features": artifact_parquet},
        registered_model_name="travelmind_enriched_model"
    )
    print("PyFunc registrado con parquet incluido")
    print("Run ID:", run.info.run_id)
