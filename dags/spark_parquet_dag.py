from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_parquet_conversion',
    default_args=default_args,
    description='Convert CSV from S3 to Parquet format',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['spark', 's3', 'parquet', 'etl'],
)

# Using SparkSubmitOperator with the configuration you provided, adapted for Python script
spark_s3 = SparkSubmitOperator(
    task_id='SparkS3ExampleClient',
    application='./include/spark_parquet_example.py',  # Path to your Python script
    conn_id='spark_standalone_client',
    executor_cores=2,
    total_executor_cores=2,
    verbose=True,
    dag=dag,
)

# Set task dependencies
spark_s3