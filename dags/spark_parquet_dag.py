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

# Option 1: Using SparkSubmitOperator (recommended)
convert_csv_to_parquet = SparkSubmitOperator(
    task_id='convert_csv_to_parquet',
    application='/app/jobs/spark_parquet_example.py',  # Path inside the container
    conn_id='spark_standalone_client',
    executor_cores=1,
    executor_memory='1g',
    driver_memory='1g',
    dag=dag,
)

# Option 2: Alternative approach using BashOperator to run docker exec
# Uncomment this if SparkSubmitOperator doesn't work with your setup
'''
convert_csv_to_parquet_bash = BashOperator(
    task_id='convert_csv_to_parquet_bash',
    bash_command='docker exec spark-master /home/sparkuser/spark/bin/spark-submit /app/jobs/spark_parquet_example.py',
    dag=dag,
)
'''

# Task to verify the results
verify_parquet_created = BashOperator(
    task_id='verify_parquet_created',
    bash_command='echo "Verification: Check S3 bucket for parquet files at s3a://bucket1/sample_parquet2"',
    dag=dag,
)

# Set task dependencies
convert_csv_to_parquet >> verify_parquet_created
# Uncomment this if using the BashOperator instead
# convert_csv_to_parquet_bash >> verify_parquet_created