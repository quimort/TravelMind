#reentrenar modelo v1
# =======================================================
# Train, register XGBoost model and wrap as PyFunc + plots
# =======================================================
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pathlib import Path
import mlflow
import mlflow.sklearn
import mlflow.pyfunc
from mlflow.models.signature import infer_signature
import xgboost as xgb
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.metrics import (
    accuracy_score, roc_auc_score, f1_score, precision_score, recall_score,
    roc_curve, precision_recall_curve, average_precision_score,
    confusion_matrix
)
from sklearn.metrics import ConfusionMatrixDisplay
from collections import Counter
from datetime import datetime
# ===========================
# 1️⃣ Cargar features
# Resolve path relative to the project root (TravelMind/) so the script can be
# executed from any working directory.
# ===========================
BASE_DIR = Path(__file__).resolve().parent.parent  # TravelMind/
artifact_parquet = BASE_DIR / "exploitation" / "travelmind_features.parquet"
if not artifact_parquet.exists():
    raise FileNotFoundError(
        f"Feature parquet not found at {artifact_parquet!s}.\n"
        "Make sure you run the feature extraction step or provide the file.\n"
        "You can also run this script from the project root: `cd TravelMind && python ml_models\\ml_model_PyFunc.py`")

df_base = pd.read_parquet(artifact_parquet)
print(f"Registros cargados: {len(df_base)}")

# ===========================
# 2️⃣ Definir labels binarias
# ===========================
# Example thresholds (you used arbitrary percentiles; keep same logic)
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
# 3️⃣ Definir features (mismo listado)
# ===========================
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

# Ensure features exist in df_base (safe-check)
missing = [c for c in feature_cols if c not in df_base.columns]
if missing:
    raise ValueError(f"Faltan columnas en el parquet: {missing}")

X_df = df_base[feature_cols].copy()
y = df_base["label"].copy()

# ===========================
# 4️⃣ Train/test split (DataFrame-based to keep column names)
# ===========================
X_train, X_test, y_train, y_test = train_test_split(
    X_df, y, test_size=0.2, random_state=42, stratify=y
)

# ===========================
# 5️⃣ Calcular ratio desbalance
# ===========================
counter = Counter(y_train)
num_pos = counter.get(1, 0)
num_neg = counter.get(0, 0)
ratio = (num_neg / num_pos) if num_pos != 0 else 1.0
print(f"Positivos: {num_pos}, Negativos: {num_neg}, Ratio (neg/pos): {ratio:.2f}")

# ===========================
# 6️⃣ Definir clasificador XGBoost
# ===========================
xgb_clf = xgb.XGBClassifier(
    objective="binary:logistic",
    eval_metric="auc",
    scale_pos_weight=ratio,
    random_state=42,
    n_jobs=-1,
    use_label_encoder=False
)

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

# Output directory for figures (and artifacts)
OUTPUT_DIR = Path("./outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def generate_dummy_plots(run_tag: str = None):
    """Create a few dummy PNGs and log them to MLflow (useful for testing artifact logging)."""
    import numpy as np

    with mlflow.start_run() as run:
        if run_tag:
            mlflow.set_tag("test_tag", run_tag)

        # Simple dummy feature importance
        features = [f"f{i}" for i in range(1, 11)]
        importance = np.random.rand(len(features))
        fig, ax = plt.subplots(figsize=(6, 4))
        sns.barplot(x=importance, y=features, ax=ax)
        ax.set_title("Dummy feature importance")
        fig_path = OUTPUT_DIR / "feature_importance_top20.png"
        fig.savefig(fig_path)
        plt.close(fig)
        mlflow.log_artifact(str(fig_path))

        # Dummy ROC curve
        x = np.linspace(0, 1, 100)
        y = np.sqrt(x) * 0.9
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(x, y, label="dummy")
        ax.plot([0, 1], [0, 1], linestyle="--", color="gray")
        ax.set_title("Dummy ROC")
        fig_path = OUTPUT_DIR / "roc_curve.png"
        fig.savefig(fig_path)
        plt.close(fig)
        mlflow.log_artifact(str(fig_path))

        # Dummy confusion matrix
        fig, ax = plt.subplots(figsize=(4, 3))
        sns.heatmap([[50, 5], [3, 42]], annot=True, fmt="d", cmap="Blues", ax=ax)
        ax.set_title("Dummy Confusion")
        fig_path = OUTPUT_DIR / "confusion_matrix.png"
        fig.savefig(fig_path)
        plt.close(fig)
        mlflow.log_artifact(str(fig_path))

        print(f"Dummy plots created and logged in run {run.info.run_id}")
        return run.info.run_id

# ===========================
# 7️⃣ Entrenar, evaluar y registrar XGBoost (con plots)
# ===========================
with mlflow.start_run() as run:
    print("Training run ID:", run.info.run_id)
    grid.fit(X_train, y_train)
    best_model = grid.best_estimator_
    print("Best params:", grid.best_params_)

    # Predictions
    y_pred = best_model.predict(X_test)
    y_pred_prob = best_model.predict_proba(X_test)[:, 1]

    # Metrics
    acc = accuracy_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_pred_prob)
    f1 = f1_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    print(f"Accuracy={acc:.3f}, AUC={auc:.3f}, F1={f1:.3f}, Precision={precision:.3f}, Recall={recall:0.3f}")

    # Log params and metrics
    mlflow.log_params(grid.best_params_)
    mlflow.log_param("scale_pos_weight", ratio)
    mlflow.log_metrics({
        "accuracy": acc,
        "auc": auc,
        "f1": f1,
        "precision": precision,
        "recall": recall
    })

    # ---- Create & save plots ----

    # 1) Feature importance (top 20)
    fi = pd.DataFrame({
        "feature": feature_cols,
        "importance": best_model.feature_importances_
    }).sort_values("importance", ascending=False)
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.barplot(data=fi.head(20), x="importance", y="feature", ax=ax)
    ax.set_title("Feature importance (XGBoost) - top 20")
    plt.tight_layout()
    fig_path = OUTPUT_DIR / "feature_importance_top20.png"
    fig.savefig(fig_path)
    mlflow.log_artifact(str(fig_path))
    plt.close(fig)

    # 2) Confusion matrix
    cm = confusion_matrix(y_test, y_pred)
    fig, ax = plt.subplots(figsize=(5, 4))
    disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=["No", "Yes"])
    disp.plot(ax=ax, cmap="Blues", colorbar=False)
    ax.set_title("Confusion matrix")
    plt.tight_layout()
    fig_path = OUTPUT_DIR / "confusion_matrix.png"
    fig.savefig(fig_path)
    mlflow.log_artifact(str(fig_path))
    plt.close(fig)

    # 3) ROC curve
    fpr, tpr, _ = roc_curve(y_test, y_pred_prob)
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot(fpr, tpr, label=f"AUC = {auc:.3f}")
    ax.plot([0, 1], [0, 1], linestyle="--", color="gray")
    ax.set_xlabel("False Positive Rate")
    ax.set_ylabel("True Positive Rate")
    ax.set_title("ROC curve")
    ax.legend()
    plt.tight_layout()
    fig_path = OUTPUT_DIR / "roc_curve.png"
    fig.savefig(fig_path)
    mlflow.log_artifact(str(fig_path))
    plt.close(fig)

    # 4) Precision-Recall curve
    precision_vals, recall_vals, _ = precision_recall_curve(y_test, y_pred_prob)
    ap = average_precision_score(y_test, y_pred_prob)
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot(recall_vals, precision_vals, label=f"AP = {ap:.3f}")
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title("Precision-Recall curve")
    ax.legend()
    plt.tight_layout()
    fig_path = OUTPUT_DIR / "precision_recall_curve.png"
    fig.savefig(fig_path)
    mlflow.log_artifact(str(fig_path))
    plt.close(fig)

    # 5) Optional: SHAP summary (if available)
    if SHAP_AVAILABLE:
        try:
            explainer = shap.TreeExplainer(best_model)
            # Use a sample (to speed up computation), or X_test if small
            X_for_shap = X_test.sample(frac=0.5, random_state=42) if len(X_test) > 500 else X_test
            shap_values = explainer.shap_values(X_for_shap) if hasattr(explainer, "shap_values") else explainer(X_for_shap)
            # Plot SHAP summary
            plt.figure(figsize=(8, 6))
            shap.summary_plot(shap_values, X_for_shap, show=False)
            fig = plt.gcf()
            fig_path = OUTPUT_DIR / "shap_summary.png"
            fig.savefig(fig_path, dpi=150, bbox_inches="tight")
            mlflow.log_artifact(str(fig_path))
            plt.close(fig)
        except Exception as e:
            print("SHAP plotting failed:", e)
            mlflow.log_param("shap_available", False)
    else:
        print("SHAP not available; skipping SHAP plots.")
        mlflow.log_param("shap_available", False)

    # ---- Log the trained best_model with example and signature ----
    # Prepare input_example and signature (using a small sample)
    input_example = X_test.head(5)
    preds_example = best_model.predict(input_example)
    signature = infer_signature(input_example, preds_example)

    mlflow.sklearn.log_model(
        sk_model=best_model,
        artifact_path="xgb_model",
        #input_example=X_test[:5],
        #signature=mlflow.models.infer_signature(X_test, y_test),
        registered_model_name="travelmind_xgb_model"
    )
    print("Modelo XGBoost registrado en MLflow")

# Guardar parquet en carpeta temporal para empaquetar (unchanged)
artifact_parquet = "./travelmind_features.parquet"
df_base.to_parquet(artifact_parquet)

# ===========================
# 8️⃣ Crear PyFunc completo que empaqueta parquet
# ===========================
class TravelMindPyFunc(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        # Cargar modelo desde MLflow Model Registry
        model_name = "travelmind_xgb_model"
        model_version = 1  # Cambia versión si hace falta
        model_uri = f"models:/{model_name}/{model_version}"

        self.model = mlflow.sklearn.load_model(model_uri)  
        # Cargar tabla base
        self.df_base = pd.read_parquet(context.artifacts.get["travelmind_features"])        

    def enrich_data(self, ciudad, fecha):
        dt = pd.to_datetime(fecha)
        month, day_number, year = dt.month, dt.weekday()+1, dt.year
        df_filtered = self.df_base[
            (self.df_base["PROVINCIA"] == ciudad) &
            (self.df_base["MES"] == month) &
            (self.df_base["dia_numero"] == day_number)
        ]
        feature_cols_local = self.df_base.columns.intersection(feature_cols)
        df_grouped = df_filtered.groupby(["PROVINCIA", "MES", "dia_numero"], as_index=False)[feature_cols_local].mean()
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
            probs = self.model.predict_proba(X_input)[:, 1]
            results.append({"ciudad": row["ciudad"], "fecha": row["fecha"], "prediction": int(preds[0]), "probability": float(probs[0])})
        return pd.DataFrame(results)

# ------------------------------------------
# 9️⃣ Guardar PyFunc con parquet incluido
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