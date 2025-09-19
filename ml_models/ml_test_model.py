from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
import utils as utils
from pyspark.sql.functions import *
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from xgboost.spark import SparkXGBClassifier
from pyspark.ml import Pipeline
from builtins import min as python_min
import mlflow
import mlflow.spark
from pyspark.ml.pipeline import PipelineModel

def start_spark():
    spark = utils.create_context()
    return spark


spark = start_spark()
# Última versión registrada
model_uri = "models:/travelmind_xgb_model/1"   # versión 1
# O bien, si usas alias:
# model_uri = "models:/travelmind_xgb_model/Production"

save_path = "file:///D:/Quim/Documents/quim documents/Master/TFM/TravelMind/ml_models/models/travelmind_xgb_model"

loaded_model = PipelineModel.load(save_path)