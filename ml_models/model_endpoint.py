import mlflow.pyfunc
import pandas as pd
from datetime import datetime
import mlflow.sklearn
import utils as utils

class TravelMindModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        model_name = "travelmind_xgb_model"
        model_version = 4
        model_uri = f"models:/{model_name}/{model_version}"
        
        self.model = mlflow.sklearn.load_model(model_uri)

        self.df_base = self.load_exploitation_table(
            db_name="exploitation",
            table_name="travelmind_features"
        )

    def enrich_data(self, ciudad: str, fecha: str) -> pd.DataFrame:
        # Convertir fecha a datetime
        dt = datetime.strptime(fecha, "%Y-%m-%d")
        year = dt.year
        month = dt.month
        day_number = dt.weekday() + 1  # Spark usa 1=Lunes ... 7=Domingo
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
        # 🔹 Filtrar DataFrame
        df_filtered = self.df_base[
            (self.df_base["PROVINCIA"] == ciudad) &
            (self.df_base["MES"] == month) &
            (self.df_base["dia_numero"] == day_number)
        ]

        # 🔹 Agrupar y promediar las columnas de features
        df_grouped = df_filtered.groupby(
            ["PROVINCIA", "MES", "dia_numero"], as_index=False
        )[feature_cols].mean()

        # 🔹 Añadir columna AÑO
        df_grouped["PROVINCIA"] = df_filtered["PROVINCIA"].astype("category")
        df_grouped["AÑO"] = year

        return df_grouped[feature_cols]

    def predict(self, context, model_input: pd.DataFrame) -> dict:

        results = []

        for _, row in model_input.iterrows():
            ciudad = row["ciudad"]
            fecha = row["fecha"]

            df_features = self.enrich_data(ciudad, fecha)
            preds = self.model.predict(df_features)
            probs = self.model.predict_proba(df_features)[:, 1]

            results.append({
                "ciudad": ciudad,
                "fecha": fecha,
                "prediction": preds[0],
                "probability": float(probs[0])
            })

        return results
    
    def load_exploitation_table(self,db_name:str, table_name: str) -> pd.DataFrame:
        df = pd.read_parquet(f"./data/warehouse/{db_name}/tm_features_for_pd/{table_name}.parquet")
        return df


# Guardar modelo PyFunc con referencia al XGBoost
with mlflow.start_run() as run:
    mlflow.pyfunc.log_model(
        artifact_path="travelmind_enriched_model",
        python_model=TravelMindModel(),
        registered_model_name="travelmind_enriched_model"
    )

    print("Modelo registrado correctamente en MLflow")
    print("Run ID:", run.info.run_id)