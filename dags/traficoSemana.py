from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import your ETL functions
from landing.traficoSemana import process_trafico_semana
from trusted.traficoSemana import process_trafico_semana_selected

# Define the DAG
with DAG(
    dag_id="trafico_semana_loadData",
    start_date=datetime(2025, 9, 25),
    schedule_interval="@daily",
    catchup=False,
    tags=["travelmind", "trafico_semana"],
) as dag:

    # --- Task 1: Load data into the Landing zone ---
    t1_landing = PythonOperator(
        task_id="load_landing_trafico_semana",
        python_callable=process_trafico_semana,
    )

    # --- Task 2: Transform and load into the Trusted zone ---
    t2_trusted = PythonOperator(
        task_id="load_trusted_trafico_semana",
        python_callable=process_trafico_semana_selected,
    )

    # --- Dependencies ---
    t1_landing >> t2_trusted
