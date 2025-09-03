from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
import utils as utils
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from xgboost.spark import SparkXGBClassifier
from pyspark.ml import Pipeline
from builtins import min as python_min
import mlflow
import mlflow.spark


def statr_experiment():
    mlflow.set_experiment("visitas-ciudad-model")

def start_spark():
    spark = utils.create_context()
    return spark



if __name__ == "__main__":
    statr_experiment()
    spark = start_spark()
    print("=== Loading source tables ===")

    # ------------------------------
    # Hoteles
    #print("  Loading hotel occupancy data...")
    #df_hotels = utils.read_iceberg_table(
    #    spark=spark, 
    #    db_name="exploitation", 
    #    table_name="f_ocupacion_barcelona"
    #)
    #print(f"    Hotel records: {df_hotels.count()}")
    print("  Loading apartamentos data...")
    df_apartamentos = utils.read_iceberg_table(
        spark=spark,
        db_name="trusted",
        table_name="apartamentosTuristicos_ocupacion_selected"
    )
    print(f"    Apartamentos records: {df_apartamentos.count()}")

    print("  Loading actividades ocio data...")
    df_ocio = utils.read_iceberg_table(
        spark=spark,
        db_name="trusted",
        table_name="actividades_Ocio_selected"
    )
    print(f"    Ocio records: {df_ocio.count()}")

    print("  Loading calidad aire data...")
    df_calidad = utils.read_iceberg_table(
        spark=spark,
        db_name="trusted",
        table_name="calidad_aire_selected"
    )
    print(f"    Calidad aire records: {df_calidad.count()}")

    print("  Loading trafico semanal data...")
    df_trafico = utils.read_iceberg_table(
        spark=spark,
        db_name="trusted",
        table_name="trafico_semana_selected"
    )
    print(f"    Trafico records: {df_trafico.count()}")

    print("  Joining datasets...")

    # Join de los datasets
    df_joined = (
        df_apartamentos.alias("a") \
        .join(df_ocio.alias("o"), (col("a.PROVINCIA") == col("o.PROVINCIA")) & (col("a.AÑO") == col("o.AÑO")) & (col("a.MES")==col("o.MES")), "left") 
        #.join(df_calidad.alias("c"), col("a.fecha") == col("c.fecha"), "left") 
        #.join(df_trafico.alias("t"), (col("a.distrito") == col("t.distrito")) & (col("a.semana") == col("t.semana")), "left") 
        #.select(
        #    col("a.fecha").alias("fecha"),
        #    col("a.distrito").alias("distrito"),
        #    col("a.ocupacion").alias("ocupacion"),
        #    col("o.num_actividades").alias("num_actividades"),
        #    col("c.ica").alias("ica"),
        #    col("t.vehiculos_dia").alias("vehiculos_dia")
        #)
    )
    print(f"    Joined records: {df_joined.count()}")

    # Manejo de