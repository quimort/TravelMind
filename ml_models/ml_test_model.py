from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
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

model_path = r"D:\Quim\Documents\quim documents\Master\TFM\TravelMind\mlruns\877728602478804183\25a24a3fc3124ec7968a25f25bb26ed2\artifacts\spark_xgb_model"
model = PipelineModel.load(model_path)