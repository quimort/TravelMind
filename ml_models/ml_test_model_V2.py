import mlflow
import mlflow.sklearn

# Cargar la versión 1 del modelo
model_name = "travelmind_xgb_model"
model_version = 4
model_uri = f"models:/{model_name}/{model_version}"

loaded_model = mlflow.sklearn.load_model(model_uri)

print(loaded_model)