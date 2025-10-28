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
        # Cargar modelo travelmind_enriched_model desde artifact
        ### NOta debe ser la uri correcta con el pkl dentro de artifacts
        model_uri = "mlruns/903138913924932597/models/m-994e9937c0c047d79afe96fd35673198/artifacts" 
        self.model = mlflow.pyfunc.load_model(model_uri)
        print("\nModelo cargado correctamente desde:", model_uri)
        
        # Cargar parquet localmente 
        # self.df_base = pd.read_parquet("mlruns/212908316257568202/models/m-32e3a42bdf3c499c966a2b6549d63ae8/artifacts/artifacts/travelmind_features.parquet")
        # Cargar tabla base desde artifact")
        parquet_path="mlruns/903138913924932597/models/m-ccc2d1b2b8974b4aaa99a9e2dcb0a496/artifacts/artifacts/travelmind_features.parquet"
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
print("\nModelo Enriquecido cargado")


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
        mensaje = "Buen momento para viajar" if pred == 1 else "Mal momento para viajar"

        return f"{mensaje} (confianza: {prob:.2f})"
    except Exception as e:
        return f"Error al predecir: {e}"

# Estilos personalizados Gradio
custom_css = """
body {background-color: #EAEAEA !important;}
.gradio-container {color: #63686B;}
input, textarea {background-color: #FFFFFF !important; color: #63686B !important;}
button {background-color: #059ED6 !important; color: #FFFFFF !important; border-radius: 8px !important;}
button:hover {background-color: #048CBC !important;}
.output-text {font-size: 1.2em; font-weight: bold;}
"""

# === FUNCIÓN PREDICCIÓN ===
def predecir(ciudad, fecha):
    try:
        df_input = pd.DataFrame({"ciudad": [ciudad], "fecha": [fecha]})
        df_pred = enriched_model.predict(df_input)
        if df_pred.empty or df_pred["prediction"].isna().all():
            return " No hay datos para la ciudad o fecha indicada."
        pred = int(df_pred["prediction"].values[0])
        prob = df_pred["probability"].values[0]
        mensaje = "🌞 Buen momento para viajar" if pred == 1 else "🌧️ No es el mejor momento para viajar"
        return f"{mensaje}\n(confianza: {prob:.2f})"
    except Exception as e:
        return f"❌ Error al predecir: {e}"


# === INTERFAZ GRADIO (diseño TravelMind mejorado) ===
with gr.Blocks(css="""
body {
    background-color: #EAEAEA !important;
}
.gradio-container {
    color: #63686B;
    font-family: 'Segoe UI', sans-serif;
}
input, textarea {
    background-color: #FFFFFF !important;
    color: #63686B !important;
}
button {
    background-color: #059ED6 !important;
    color: #FFFFFF !important;
    border-radius: 8px !important;
}
button:hover {
    background-color: #048CBC !important;
}
.output-text {
    font-size: 1.2em;
    font-weight: bold;
    color: #63686B;
    text-align: center;
}
#logo {
    display: flex;
    justify-content: center;
    margin-top: 10px;
}
#title {
    text-align: center;
    color: #059ED6;
    font-weight: 800;
    font-size: 1.8em;
    margin-top: 10px;
}
#desc {
    text-align: center;
    color: #63686B;
    font-size: 1.1em;
    margin-bottom: 25px;
}
""") as demo:

    # Asegúrate que el logo esté en la misma carpeta del script o usa ruta absoluta
    gr.HTML("""
    <div id="logo">
        <img src="file=C:/Users/varga/Desktop/MasterBIGDATA_BCN/Aulas/Proyecto/TFM/TravelMind/TravelMind/WebApp/travelmind_logo.png" alt="TravelMind Logo" width="140" style="border-radius:16px;">
    </div>
    """)
    gr.HTML("<div id='title'>TravelMind – Recomendador de Destinos</div>")
    gr.HTML("<div id='desc'>Predice si es un buen momento para viajar según clima, aforo y percepción</div>")

    with gr.Row():
        ciudad = gr.Textbox(label="🏙️ Ciudad", placeholder="Ejemplo: Barcelona")
        fecha = gr.Textbox(label="📅 Fecha (YYYY-MM-DD)", placeholder="2025-08-15")

    with gr.Row():
        boton = gr.Button("🔍 Predecir", elem_id="boton")
        salida = gr.Textbox(label="Resultado", placeholder="Aquí aparecerá la recomendación", elem_classes=["output-text"])

    boton.click(fn=predecir, inputs=[ciudad, fecha], outputs=salida)


if __name__ == "__main__":
    print("\nIniciando Gradio...")
    demo.launch(share=False)#share=True para compartir online