import os
import mlflow
import mlflow.pyfunc
import pandas as pd
import gradio as gr
import numpy as np


# Apuntar MLflow a tu carpeta local de mlruns
mlflow.set_tracking_uri("./mlruns")

# Modelos PyFunc
class TravelMindPyFuncLocal(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        # Cargar modelo XGBoost normal desde artifact
        # Cargar el modelo enriquecido (Enriched) por versión
        #model_name = "travelmind_xgb_model"
        #model_version = 2
        #model_uri = f"models:/{model_name}/{model_version}"
        # Cargar modelo XGBoost normal desde artifact
        model_uri = "mlruns/212908316257568202/models/m-0a13029e27f3467c9c7eb5858d0af94c/artifacts" 
        self.model = mlflow.pyfunc.load_model(model_uri)
        
        # Cargar parquet localmente 
        # self.df_base = pd.read_parquet("mlruns/212908316257568202/models/m-32e3a42bdf3c499c966a2b6549d63ae8/artifacts/artifacts/travelmind_features.parquet")
        # Cargar tabla base desde artifact")
        parquet_path="mlruns/212908316257568202/models/m-409a75f00ab347e1b84deaaa7f40410d/artifacts/artifacts/travelmind_features.parquet"
        self.df_base = pd.read_parquet(parquet_path)
        #self.df_base["dia_numero"] = pd.to_datetime(self.df_base["FECHA"]).dt.weekday + 1

    def enrich_data(self, ciudad, fecha):
        dt = pd.to_datetime(fecha)
        month=dt.month
        year = dt.year
        day_number=dt.weekday()+1 # Spark usa 1=Lunes ... 7=Domingo
        df_filtered = self.df_base[
            (self.df_base["PROVINCIA"]==ciudad) &
            (self.df_base["MES"]==month)
            #(self.df_base["dia_numero"]==day_number)
        ]
        feature_cols_local = self.df_base.columns.intersection(
            [
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
            ])
        #df_grouped = df_filtered.groupby(["PROVINCIA","MES", "dia_numero"], as_index=False)[feature_cols_local].mean()
        df_grouped = df_filtered.groupby(["PROVINCIA","MES"], as_index=False)[feature_cols_local].mean()
        df_grouped["AÑO"] = year
        return df_grouped[feature_cols_local]

    def predict(self, model_input: pd.DataFrame):
        results = []
        for _, row in model_input.iterrows():
            X_input = self.enrich_data(row["ciudad"], row["fecha"])
            if X_input.empty:
                print(f"No se encontraron datos para {row['ciudad']} en {row['fecha']}")
                results.append({"ciudad": row["ciudad"], "fecha": row["fecha"], "prediction": None, "probability": None})
                continue
            preds = self.model.predict(X_input)        

# Si el modelo devuelve probabilidades directamente
            if isinstance(preds[0], (float, np.floating)):
                probs = preds
                preds = [1 if p > 0.5 else 0 for p in probs]
            else:
                # Si devuelve clases, asumimos prob=1.0 para 'Yes'
                probs = [1.0 if p == 1 else 0.0 for p in preds]
            results.append({"ciudad": row["ciudad"], "fecha": row[  "fecha"], "prediction": int(preds[0]), "probability": float(probs[0])})
        return pd.DataFrame(results)


# Cargar modelo enriquecido
enriched_model = TravelMindPyFuncLocal()
enriched_model.load_context(None)
print("Modelo Enriquecido cargado")


###INTERFAZ GRADIO

# Función para la interfaz
def predecir(ciudad, fecha):
    try:
        df_input = pd.DataFrame({"ciudad":[ciudad], "fecha":[fecha]})
        df_pred = enriched_model.predict(df_input)
    
        if df_pred.empty or df_pred['prediction'].isna().all():
            return "No hay datos para la ciudad/fecha indicada"
        
        pred = int(df_pred['prediction'].values[0])
        prob = df_pred['probability'].values[0]

        # Traducción de 0/1 a texto
        mensaje = "Buen momento para viajar" if pred == 0 else "Mal momento para viajar"

        return f"{mensaje} (confianza: {prob:.2f})"
    except Exception as e:
        return f"Error al predecir: {e}"
# Interfaz Gradio
demo = gr.Interface(
    fn=predecir,
    inputs=[gr.Textbox(label="Ciudad"), gr.Textbox(label="Fecha (YYYY-MM-DD)")],
    outputs="text"
)

if __name__ == "__main__":
    demo.launch()#share=True para compartir online