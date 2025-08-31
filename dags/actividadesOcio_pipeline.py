from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from landing.actividadesOcio import process_actividades

from random import randint
from datetime import datetime

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'


def _training_model():
    return randint(1, 10)

with DAG("my_dag", start_date=datetime(2021, 1, 1),
    schedule_interval="@daily", catchup=False) as dag:

        training_model_A = PythonOperator(
            task_id="training_model_A",
            python_callable=_training_model
        )

        training_model_B = PythonOperator(
            task_id="training_model_B",
            python_callable=_training_model
        )

        training_model_C = PythonOperator(
            task_id="training_model_C",
            python_callable=_training_model
        )

        choose_best_model = BranchPythonOperator(
            task_id="choose_best_model",
            python_callable=_choose_best_model
        )

        accurate = BashOperator(
            task_id="accurate",
            bash_command="echo 'accurate'"
        )

        inaccurate = BashOperator(
            task_id="inaccurate",
            bash_command="echo 'inaccurate'"
        )
        t1 = PythonOperator(
    task_id="procesa_actividades",
    python_callable=process_actividades,
    op_kwargs={
        "path": "https://dataestur.azure-api.net/API-SEGITTUR-v1/ACTIVIDADES_OCIO_DL?CCAA=Todos&Provincia=Todos",
        # no pases spark: deja que la tarea cree su propia SparkSession en el contenedor
        "db_name": "landing",
        "table_name": "actividades_Ocio",
        "show_rows": 10,
    },
)

        [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate] >> t1