import mlflow
import mlflow.sklearn
import pandas as pd
import shap
from datetime import datetime
import numpy as np

def enrich_data(df_base:pd.DataFrame, ciudad: str, fecha: str) -> pd.DataFrame:
    # Convertir fecha a datetime
    dt = datetime.strptime(fecha, "%Y-%m-%d")
    year = dt.year
    month = dt.month
    day_number = dt.weekday() + 1  # Spark usa 1=Lunes ... 7=Domingo
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
    # 🔹 Filtrar DataFrame
    df_filtered = df_base[
        (df_base["PROVINCIA"] == ciudad) &
        (df_base["MES"] == month) &
        (df_base["dia_numero"] == day_number)
    ]

    # 🔹 Agrupar y promediar las columnas de features
    df_grouped = df_filtered.groupby(
        ["PROVINCIA", "MES", "dia_numero"], as_index=False
    )[feature_cols].mean()

    # 🔹 Añadir columna AÑO
    df_grouped["PROVINCIA"] = df_filtered["PROVINCIA"].astype("category")
    df_grouped["AÑO"] = year

    return df_grouped[feature_cols]

def predict(model, context, model_input: pd.DataFrame) -> dict:

    results = []

    for _, row in model_input.iterrows():
        ciudad = row["ciudad"]
        fecha = row["fecha"]

        df_features = enrich_data(ciudad, fecha)
        preds = model.predict(df_features)
        probs = model.predict_proba(df_features)[:, 1]

        results.append({
            "ciudad": ciudad,
            "fecha": fecha,
            "prediction": preds[0],
            "probability": float(probs[0])
        })

    return results

def load_exploitation_table(db_name:str, table_name: str) -> pd.DataFrame:
    df = pd.read_parquet(f"./data/warehouse/{db_name}/tm_features_for_pd/{table_name}.parquet")
    return df

def top_features(shap_row, feature_names, n=3):
    idx = np.argsort(np.abs(shap_row))[::-1][:n]
    return ", ".join([feature_names[i] for i in idx])

if __name__ == "__main__":

    # Cargar la versión 1 del modelo
    model_name = "travelmind_xgb_model"
    model_version = 9
    model_uri = f"models:/{model_name}/{model_version}"

    model = mlflow.xgboost.load_model(model_uri)

    print(type(model))

    features_data = load_exploitation_table(
        db_name="exploitation",
        table_name="travelmind_features"
    )
    enriched_data = enrich_data(
        df_base=features_data,
        ciudad="Barcelona",
        fecha="2025-10-26"
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
    # Predicciones
    df_pred = enriched_data.copy()  # si estás en PySpark
    X_test = df_pred[feature_cols]
    df_pred["pred"] = model.predict(X_test)
    df_pred["prob_bueno"] = model.predict_proba(X_test)[:, 1]

    booster = model.get_booster() 
    # Crear el objeto explicador
    explainer = shap.TreeExplainer(booster)
    # Calcular valores SHAP para el conjunto de test
    shap_values = explainer.shap_values(X_test)

    df_pred["explicacion"] = [
        top_features(shap_values[i], X_test.columns, n=3)
        for i in range(len(X_test))
    ]

    shap.summary_plot(shap_values, X_test)