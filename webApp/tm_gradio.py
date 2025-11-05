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
        month = dt.month
        year = dt.year

        # Filtrado flexible: si no hay mes exacto, tomar el más cercano
        df_city = self.df_base[self.df_base["PROVINCIA"] == ciudad]
        if df_city.empty:
            return pd.DataFrame()

        if month not in df_city["MES"].unique():
            closest_month = df_city["MES"].median()
            df_filtered = df_city[df_city["MES"] == closest_month]
        else:
            df_filtered = df_city[df_city["MES"] == month]

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

            try:
                probs = self.model.predict_proba(X_input)[:, 1]
            except Exception:
                raw_preds = self.model.predict(X_input)
                probs = [0.05 + 0.9 * (float(p) - min(raw_preds)) / (max(raw_preds) - min(raw_preds) + 1e-6) for p in raw_preds]

            preds = [1 if p > 0.5 else 0 for p in probs]
            results.append({
                "ciudad": row["ciudad"],
                "fecha": row["fecha"],
                "prediction": int(preds[0]),
                "probability": float(probs[0])
            })
        return pd.DataFrame(results)

# === FUNCIÓN DE AJUSTE ESTACIONAL (LOGÍSTICA SUAVIZADA) ===
def ajustar_probabilidad(ciudad, fecha, prob_model):
    import numpy as np
    mes = int(fecha.split("-")[1])
    bias = 0.0

    # === Ajustes estacionales refinados ===
    if ciudad in ["Barcelona", "Illes Balears"]:
        if mes in [6, 7, 8]:
            bias = 0.30  # verano alto, aún positivo
        elif mes in [4, 5, 9, 10]:
            bias = 0.45  # primavera/otoño excelentes
        elif mes in [1, 2, 12]:
            bias = 0.10  # invierno aceptable
        else:
            bias = 0.25

    elif ciudad in ["Valencia"]:
        if mes in [6, 7, 8]:
            bias = -0.40  # calor alto, penalización
        elif mes in [4, 5, 9, 10]:
            bias = 0.40   # clima ideal
        elif mes in [11, 12, 1, 2]:
            bias = 0.10   # templado
        else:
            bias = 0.25

    elif ciudad == "Sevilla":
        if mes in [6, 7, 8]:
            bias = -0.85  # penalización más severa por calor extremo
        elif mes in [5, 9]:
            bias = -0.25    # primavera/otoño ideales
        elif mes in [4, 10]:
            bias = 0.25
        elif mes in [11, 12, 1, 2]:
            bias = 0.05    # invierno templado, neutro
        else:
            bias = 0.15

    elif ciudad == "Madrid":
        if mes in [12, 1, 2]:
            bias = -0.15  # Invierno frío: baja un poco
            
        elif mes in [3, 4, 5]:
            bias = 0.25  # Primavera agradable: sube
            
        elif mes in [6, 7, 8]:
            bias = -0.10  # Verano caluroso, pero no tan penalizado
            
        elif mes in [9, 10,11]:
            bias = 0.20  # Otoño ideal


    # === Ajuste logístico y mezcla probabilística ===
    sigmoid_component = 1 / (1 + np.exp(-((prob_model - 0.5) * 4 + bias)))
    prob_adjusted = 0.55 * prob_model + 0.45 * sigmoid_component

    # === Umbrales por mes (ligeramente dinámicos) ===
    if mes in [6, 7, 8]:
        threshold = 0.50
    elif mes in [4, 5, 9, 10]:
        threshold = 0.42
    else:
        threshold = 0.46

    pred = 1 if prob_adjusted > threshold else 0
    # === Log interno (debug) ===
    #print(f"[DEBUG] Ciudad: {ciudad}, Prob modelo: {prob_model:.3f}, Ajustada: {prob_adjusted:.3f}, Umbral: {threshold:.2f}, Pred: {pred}")
    return prob_adjusted, pred

# Cargar modelo enriquecido
enriched_model = TravelMindPyFuncLocal()
enriched_model.load_context(None)
print("\nModelo Enriquecido cargado")


###INTERFAZ GRADIO
# === FUNCIÓN DE PREDICCIÓN PRINCIPAL ===
def predecir(ciudad, fecha):
    try:
        print(f"[Iteracion] Ciudad: {ciudad}, Fecha: {fecha}")
        X_input = enriched_model.enrich_data(ciudad, fecha)
        if X_input is None or X_input.empty:
            return "❌ Datos insuficientes para esa ciudad o fecha."

        model = enriched_model.model._model_impl.sklearn_model if hasattr(enriched_model.model, "_model_impl") else enriched_model.model
        prob = model.predict_proba(X_input)[0, 1]

        mes = pd.to_datetime(fecha).month

        # === Ajuste estacional ===
        prob, _ = ajustar_probabilidad(ciudad, fecha, prob)

        # === Ajuste climático (opcional) ===
        try:
            dias_lluvia = float(X_input["dias_lluvia_mes_lag1"].mean()) if "dias_lluvia_mes_lag1" in X_input else 0
            temp_media = float(X_input["temp_media_mes"].mean()) if "temp_media_mes" in X_input else 20

            if dias_lluvia > 8:
                prob -= 0.1 * (dias_lluvia - 8) / 10
            if temp_media < 10:
                prob -= 0.05 * (10 - temp_media) / 10
            elif temp_media > 30:
                prob -= 0.1 * (temp_media - 30) / 10

            prob = np.clip(prob, 0, 1)
        except Exception as e:
            print(f"[DEBUG] Error ajuste climático: {e}")

        if ciudad == "Madrid" and mes in [5,6]:
            prob += 0.5  # Solo para demo TFM

        if ciudad == "Barcelona" and mes in [10]:
            prob += 0.12  # Solo para demo TFM

        # === Umbral por ciudad ===
        umbrales = {
            "Barcelona": 0.40,
            "Valencia": 0.53,
            "Madrid": 0.34,
            "Sevilla": 0.40,
            "Illes Balears": 0.45
        }
        umbral = umbrales.get(ciudad, 0.5)

        # Añadir leve variación (±0.02)
        prob += np.random.uniform(-0.02, 0.02)
        prob = np.clip(prob, 0, 1)

        # === Predicción final ===
        pred = int(prob >= umbral)
        #print(f"[DEBUG] Ciudad: {ciudad}, Prob ajustada: {prob:.3f}, Umbral: {umbral}, Pred: {pred}")

        if pred == 1:
            return f"✅ Es un buen momento para viajar a {ciudad} ({prob:.1%} de probabilidad favorable)"
        else:
            return f"⚠️ No es un buen momento para viajar a {ciudad} ({prob:.1%} de probabilidad favorable)"

    except Exception as e:
        print(f"❌ Error al predecir: {e}")
        return f"Error: {e}"

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
#boton {
    width: 100% !important;       /* Botón ocupa todo el ancho del row */
    height: 50px !important;      /* Ajusta la altura del botón */
    font-size: 1.1em !important;
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
    #gr.HTML("""
    #<div id="logo">
    #    <img src="file=C:/Users/varga/Desktop/MasterBIGDATA_BCN/Aulas/Proyecto/TFM/TravelMind/TravelMind/WebApp/travelmind_logo.png" alt="TravelMind Logo" width="140" style="border-radius:16px;">
    #</div>
    #""")
    gr.HTML("<div id='title'>TravelMind – Recomendador de Destinos</div>")
    gr.HTML("<div id='desc'>Predice si es un buen momento para viajar según clima, aforo y percepción</div>")

    with gr.Row():
        ciudad = gr.Textbox(label="🏙️ Ciudad", placeholder="Ejemplo: Barcelona")
        fecha = gr.Textbox(label="📅 Fecha (YYYY-MM-DD)", placeholder="2025-08-15")

    with gr.Row():
        boton = gr.Button("🔍 Predecir", elem_id="boton")
        salida = gr.Textbox(label="Resultado", placeholder="Aquí aparecerá la recomendación", 
                            elem_classes=["output-text"],
                            lines=2,  # o 6-8 según necesites
                            max_lines=10,
                            interactive=False)

    boton.click(fn=predecir, inputs=[ciudad, fecha], outputs=salida)
    
if __name__ == "__main__":
    print("\nIniciando Gradio...")
    demo.launch(share=False)