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
    'spark_minio_test',
    default_args=default_args,
    description='Simple Spark MinIO CSV to Parquet conversion',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['spark', 'minio', 'etl'],
)

# Task to run the Spark-MinIO test
convert_csv_to_parquet = SparkSubmitOperator(
    task_id='convert_csv_to_parquet',
    application='/app/test_spark_minio_simple.py',
    conn_id='spark_standalone_client',
    # Remove this line - JARs are already in the Spark cluster!
    # jars='/home/sparkuser/spark/jars/hadoop-aws-3.3.4.jar,/home/sparkuser/spark/jars/aws-java-sdk-bundle-1.12.262.jar',
    executor_cores=1,
    executor_memory='1g',
    driver_memory='1g',
    dag=dag,
)

# Task to verify the results
verify_parquet_created = BashOperator(
    task_id='verify_parquet_created',
    bash_command='echo \"Verification: Check MinIO bucket for parquet files\"',
    dag=dag,
)

# Set task dependencies
convert_csv_to_parquet >> verify_parquet_created