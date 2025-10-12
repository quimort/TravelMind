from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import your ETL functions
from landing.apartamentos import process_apartamentos_ocupacion
from trusted.apartamentos import process_apartamentos_trusted

# Define the DAG
with DAG(
    dag_id="apartamentos_loadData",
    start_date=datetime(2025, 9, 25),
    schedule_interval="@daily",
    catchup=False,
    tags=["travelmind", "apartamentos"],
) as dag:

    # --- Task 1: Load data into the Landing zone ---
    t1_landing = PythonOperator(
        task_id="load_landing_apartamentos",
        python_callable=process_apartamentos_ocupacion,
        op_kwargs={
            "path": "https://dataestur.azure-api.net/API-SEGITTUR-v1/EOAP_PROVINCIA_DL?Lugar%20de%20residencia=Todos&Provincia=Todos",
            "db_name": "landing",
            "table_name": "apartamentos_ocupacion",
            "show_rows": 10,
        },
    )

    # --- Task 2: Transform and load into the Trusted zone ---
    t2_trusted = PythonOperator(
        task_id="load_trusted_apartamentos",
        python_callable=process_apartamentos_trusted,
        op_kwargs={
            "src_db": "landing",
            "src_table": "apartamentos_ocupacion",
            "tgt_db": "trusted",
            "tgt_table": "apartamentos_ocupacion_selected",
            "show_rows": 10,
        },
    )

    # --- Dependencies ---
    t1_landing >> t2_trusted
