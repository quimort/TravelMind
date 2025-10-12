from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import your ETL functions
from landing.aemetClimaDiario import process_aemet_clima_diario
from trusted.aemetClimaDiarioTrusted import process_aemet_clima_diario_selected

# Define the DAG
with DAG(
    dag_id="aemet_clima_diario_loadData",
    start_date=datetime(2025, 9, 25),
    schedule_interval="@daily",
    catchup=False,
    tags=["travelmind", "aemet_clima_diario"],
) as dag:

    # --- Task 1: Load data into the Landing zone ---
    t1_landing = PythonOperator(
        task_id="load_landing_aemet_clima_diario",
        python_callable=process_aemet_clima_diario,
    )

    # --- Task 2: Transform and load into the Trusted zone ---
    t2_trusted = PythonOperator(
        task_id="load_trusted_aemet_clima_diario",
        python_callable=process_aemet_clima_diario_selected,
    )

    # --- Dependencies ---
    t1_landing >> t2_trusted
