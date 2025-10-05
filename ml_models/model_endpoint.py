import mlflow.pyfunc
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, dayofweek, col
import mlflow.sklearn
import utils as utils

class TravelMindModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        model_name = "travelmind_xgb_model"
        model_version = 4
        model_uri = f"models:/{model_name}/{model_version}"
        
        self.model = mlflow.sklearn.load_model(model_uri)

        # Iniciar Spark
        spark = utils.create_context()

    def enrich_data(self, ciudad: str, fecha: str):
        # Convertir fecha a datetime
        dt = datetime.strptime(fecha, "%Y-%m-%d")
        mes = dt.month
        dia_semana = dt.weekday() + 1  # Spark usa 1=Lunes ... 7=Domingo

        # 🔹 Filtrar Spark DataFrame
        df_filtered = (
            self.df_base
            .filter((col("ciudad") == ciudad) &
                    (month(col("fecha")) == mes) &
                    (dayofweek(col("fecha")) == dia_semana))
        )

        # 🔹 Agregar (promediar o sumar) las features relevantes
        df_agg = df_filtered.groupBy().avg(
            "apt_viajeros", "trafico_imd_total", "temp_media_mes",
            "ocio_total_entradas", "aire_pct_buena"
        ).toPandas()

        # Cambiar nombres si Spark agrega "avg()"
        df_agg.columns = [c.replace("avg(", "").replace(")", "") for c in df_agg.columns]
        return df_agg

    def predict(self, context, model_input):
        ciudad = model_input.get("ciudad")
        fecha = model_input.get("fecha")

        # 🔹 Enriquecer datos dinámicamente
        df_features = self.enrich_data(ciudad, fecha)

        # 🔹 Predecir con el modelo XGBoost
        preds = self.model.predict(df_features)
        probs = self.model.predict_proba(df_features)[:, 1]

        return pd.DataFrame({
            "ciudad": [ciudad],
            "fecha": [fecha],
            "prediction": preds,
            "probability": probs
        })
    
    def crate_base_df(self):
        # Cargar datos base desde CSV
        spark = utils.create_context()
        self.df_base = spark.read.csv("data/travelmind_base_data.csv", header=True, inferSchema=True)
        self.df_base = self.df_base.withColumn("fecha", col("fecha").cast("date"))

# Guardar modelo PyFunc con referencia al XGBoost
mlflow.pyfunc.save_model(
    path="travelmind_enriched_model",
    python_model=TravelMindModel(),
    artifacts={"xgb_model": "runs:/be374f43b403494fa03cbcc5a6cd22a5/xgb_model"}
)
