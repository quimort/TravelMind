from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from landing.actividadesOcio import process_actividades
from trusted.actividadesOcio import process_actividades_selected

from random import randint
from datetime import datetime

with DAG("actividadesOcio_loadData", start_date=datetime(2025, 9, 25),
    schedule_interval="@daily", catchup=False) as dag:
        t1 = PythonOperator(
    task_id="LANDING",
    python_callable=process_actividades,
    op_kwargs={
        "path": "https://dataestur.azure-api.net/API-SEGITTUR-v1/ACTIVIDADES_OCIO_DL?CCAA=Todos&Provincia=Todos",
        "db_name": "landing",
        "table_name": "actividades_Ocio",
        "show_rows": 10,
    },
)
        t2 = PythonOperator(
        task_id="TRUSTED",
        python_callable=process_actividades_selected,
        provide_context=True
    )

        t1 >> t2