from pyspark.sql import SparkSession,Row,DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
import utils as utils
from pyspark.sql.functions import *
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from xgboost.spark import SparkXGBClassifier
from pyspark.ml import Pipeline,PipelineModel
from builtins import min as python_min
import mlflow
import mlflow.spark
from datetime import date

spark = utils.create_context()


db_name = "exploitation"
table_name = "travelmind_features"

df = utils.read_iceberg_table(spark,db_name,table_name)
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
        "dias_calidos", "dias_helada"
    ]
df.describe().show(truncate=False, vertical=True)
#df.show(truncate=False,n=1000)
