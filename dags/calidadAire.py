from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import your ETL functions
from landing.calidadAire import process_calidad_aire
from trusted.calidadAire import process_calidad_aire_selected

# Define the DAG
with DAG(
    dag_id="calidad_aire_loadData",
    start_date=datetime(2025, 9, 25),
    schedule_interval="@daily",
    catchup=False,
    tags=["travelmind", "calidad_aire"],
) as dag:

    # --- Task 1: Load data into the Landing zone ---
    t1_landing = PythonOperator(
        task_id="load_landing_calidad_aire",
        python_callable=process_calidad_aire,
        op_kwargs={
            "path": "https://dataestur.azure-api.net/API-SEGITTUR-v1/CALIDAD_AIRE_DL?CCAA=Todos&Provincia=Todos",
            "db_name": "landing",
            "table_name": "calidad_aire",
            "show_rows": 10,
        },
    )

    # --- Task 2: Transform and load into the Trusted zone ---
    t2_trusted = PythonOperator(
        task_id="load_trusted_apartamentos",
        python_callable=process_calidad_aire_selected,
        op_kwargs={
            "src_db": "landing",
            "src_table": "calidad_aire",
            "tgt_db": "trusted",
            "tgt_table": "calidad_aire_selected",
            "show_rows": 10,
        },
    )

    # --- Dependencies ---
    t1_landing >> t2_trusted
