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


def statr_experiment():
    mlflow.set_experiment("visitas-ciudad-model")

def start_spark():
    spark = utils.create_context()
    return spark
def convert_time_to_decimal(col_name):
    return (
        (split(col(col_name), ":").getItem(0).cast("double")) + 
        (split(col(col_name), ":").getItem(1).cast("double") / 60.0)
    )
def convert_decimal_to_time(col_name):
    hours = floor(col(col_name)).cast("int")
    minutes = round((col(col_name) - hours) * 60).cast("int")
    
    return (
        format_string("%02d:%02d", hours, minutes)
    )

def load_exploitation_table(spark: SparkSession,db_name:str, table_name: str) -> DataFrame:
    df = utils.read_iceberg_table(
        spark=spark,
        db_name=db_name,
        table_name=table_name
    )
    return df

if __name__ == "__main__":
    statr_experiment()
    spark = start_spark()
    print("=== Loading source table ===")

    df_joined = load_exploitation_table(
        spark=spark,
        db_name="exploitation",
        table_name="travelmind_features"
    )
    print(f"    Loaded records: {df_joined.count()}")
    # 1. Calcular percentiles 33 y 66
    quantiles = df_joined.approxQuantile(
        ["trafico_imd_total", "aire_pct_buena", "apt_availability_score"],
        [0.33, 0.66],
        0.01
    )

    (traf_p33, traf_p66), (aire_p33, aire_p66), (apt_p33, apt_p66) = quantiles

    print("Umbrales tráfico:", traf_p33, traf_p66)
    print("Umbrales aire:", aire_p33, aire_p66)
    print("Umbrales alojamiento:", apt_p33, apt_p66)
    # Definir percentiles previamente calculados de temperatura y precipitación
    temp_p10 = 5   # ejemplo: 10ºC
    temp_p90 = 30  # ejemplo: 30ºC
    dias_lluvia = 10  # ejemplo: 10mm (percentil 66)
    # 2. Definir la label binaria
    df_labeled = df_joined.withColumn(
        "label",
        when(
            (col("trafico_imd_total") > traf_p66) |
            (col("aire_pct_buena") < aire_p33) |
            (col("apt_availability_score") < apt_p33) |
            (col("temp_min_media_mes") < temp_p10) |   
            (col("temp_max_media_mes") > temp_p90) |   
            (col("dias_lluvia_mes") > dias_lluvia),
            0  # Malo
        ).otherwise(1)  # Bueno
    )

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

    df_labeled = df_labeled.fillna(0, subset=feature_cols)
    # VectorAssembler para convertir a vector de features
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )
    # Contar positivos (label=1) y negativos (label=0)
    counts = df_labeled.groupBy("label").count().collect()

    # Inicializamos variables
    num_positivos = 0
    num_negativos = 0

    for row in counts:
        if row['label'] == 1:
            num_positivos = row['count']
        else:
            num_negativos = row['count']

    # Ratio para scale_pos_weight
    ratio_negativos_sobre_positivos = num_negativos / num_positivos
    # 4. Definir modelo XGBoost
    xgb = SparkXGBClassifier(
        features_col="features",       # <- snake_case
        label_col="label",
        prediction_col="prediction",
        probability_col="probability",
        num_round=50,
        max_depth=5,
        eta=0.1,
        scale_pos_weight=ratio_negativos_sobre_positivos
    )

    # 5. Construir pipeline
    pipeline = Pipeline(stages=[assembler, xgb])

    # 6. Entrenar modelo
    train_df, test_df = df_labeled.randomSplit([0.8, 0.2], seed=42)
    print(f"number of considered registers: {df_labeled.count()}")
    #model = pipeline.fit(train)
    #
    ## 7. Evaluar modelo
    #predictions = model.transform(test)
    #predictions.select("año", "mes", "label", "probability", "prediction").show(10, truncate=False)
    # Evaluador
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )

    # Grid de hiperparámetros
    paramGrid = (ParamGridBuilder()
        .addGrid(xgb.getParam("max_depth"), [3, 5])
        .addGrid(xgb.getParam("learning_rate"), [0.1,0.01])
        .addGrid(xgb.getParam("n_estimators"), [50]) 
        .build()
    )

    # CrossValidator
    crossval = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=2,
        parallelism=2
    )

    with mlflow.start_run() as run:
        # Entrenar
        model = crossval.fit(train_df)
        preds = model.transform(test_df)

        # Evaluación
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="accuracy"
        )
        acc = evaluator.evaluate(preds)
        # AUC
        evaluator_auc = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="prediction", metricName="areaUnderROC"
        )
        auc = evaluator_auc.evaluate(preds)

        # F1
        evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="f1"
        )
        f1 = evaluator_f1.evaluate(preds)

        # Precision
        evaluator_precision = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
        )
        precision = evaluator_precision.evaluate(preds)

        # Recall
        evaluator_recall = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="weightedRecall"
        )
        recall = evaluator_recall.evaluate(preds)

        print(f"Accuracy: {acc:.3f}")
        print(f"AUC: {auc:.3f}")
        print(f"F1: {f1:.3f}")
        print(f"Precision: {precision:.3f}")
        print(f"Recall: {recall:.3f}")
        # Log de parámetros, métricas y modelo
        mlflow.log_param("max_depth", 5)
        mlflow.log_param("eta", 0.1)
        mlflow.log_param("num_round", 50)
        mlflow.log_param("scale_pos_weight", ratio_negativos_sobre_positivos)
        mlflow.log_metric("accuracy", acc)

        # Loggear modelo Spark
        best_model = model.bestModel  # extraer el mejor del CrossValidator
        # If best_model is a stage instead of PipelineModel
        if not isinstance(best_model, PipelineModel):
            best_model = PipelineModel(stages=[best_model])
        mlflow.spark.log_model(best_model, "spark_xgb_model")

        # Registrar en el Model Registry con un nombre legible
        mlflow.register_model(
            model_uri=f"runs:/{run.info.run_id}/spark_xgb_model",
            name="travelmind_xgb_model"
        )

        print("Modelo registrado en MLflow con run_id:", run.info.run_id)
    
    # Supongamos que queremos predecir para Barcelona, 20/09/2025
    ciudad = "Barcelona"
    fecha = "2025-10-01"
    year, month, day = map(int, fecha.split("-"))
    # Crear objeto date
    d = date(year, month, day)

    # Obtener número del día (lunes=1 ... domingo=7)
    num_dia = d.weekday() + 1
    future_df = (
        df_labeled
        .filter(
            (col("PROVINCIA") == ciudad) &
            (col("MES") == month) &
            (col("dia_numero") == num_dia)
        )
        .groupBy("PROVINCIA", "MES","dia_numero")
        .agg(
            *[avg(c).alias(c) for c in feature_cols]
        )
        .withColumn("AÑO", lit(year))
    )

    prediction = best_model.transform(future_df)

    # Mostrar resultado
    prediction.select("PROVINCIA","MES","AÑO","prediction","probability").show(truncate=False)

    # Aplica el modelo
    preds = best_model.transform(test_df)

    # Filtra los casos que el modelo predijo como "Bueno" (1)
    preds_buenos = preds.filter(col("prediction") == 1)

    # Muestra algunos resultados
    print(preds_buenos.count())
    print(preds.count())
    preds_buenos.select("AÑO", "MES","PROVINCIA","dia_numero", "prediction", "probability").show(20, truncate=False)
    