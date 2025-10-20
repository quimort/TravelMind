import cloudpickle

MODEL_PATH = r"C:\Users\varga\Desktop\MasterBIGDATA_BCN\Aulas\Proyecto\TFM\TravelMind\TravelMind\travelmind_enriched_model\python_model.pkl"

print(f"Cargando objeto PythonModel desde: {MODEL_PATH}")

with open(MODEL_PATH, "rb") as f:
    python_model = cloudpickle.load(f)

print("✅ Archivo .pkl cargado correctamente como objeto Python")

# Ver qué contiene
print("Tipo del objeto:", type(python_model))
print("Atributos disponibles:", dir(python_model))