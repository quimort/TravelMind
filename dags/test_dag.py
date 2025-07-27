from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments
default_args = {
    'owner': 'travelmind',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'test_travelmind_stack',
    default_args=default_args,
    description='Test DAG for TravelMind stack validation',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'validation'],
)

def test_python_environment():
    """Test Python environment and imports"""
    import sys
    import pyspark
    print(f"Python version: {sys.version}")
    print(f"PySpark version: {pyspark.__version__}")
    return "Python environment OK"

def test_minio_connection():
    """Test MinIO connection"""
    try:
        import boto3
        from botocore.client import Config
        
        # Configure S3 client for MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='testuser',
            aws_secret_access_key='password',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # List buckets
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        print(f"Available buckets: {buckets}")
        
        return f"MinIO connection OK - Found {len(buckets)} buckets"
    except Exception as e:
        raise Exception(f"MinIO connection failed: {str(e)}")

# Task 1: Test Python environment
test_python_task = PythonOperator(
    task_id='test_python_environment',
    python_callable=test_python_environment,
    dag=dag,
)

# Task 2: Test MinIO connection
test_minio_task = PythonOperator(
    task_id='test_minio_connection',
    python_callable=test_minio_connection,
    dag=dag,
)

# Task 3: Test Spark job
test_spark_task = SparkSubmitOperator(
    task_id='test_spark_job',
    application='/opt/spark/examples/jars/spark-examples_2.12-3.5.5.jar',
    java_class='org.apache.spark.examples.SparkPi',
    application_args=['10'],
    conn_id='spark_standalone_client',
    dag=dag,
)

# Task 4: Test Spark-MinIO integration
test_integration_task = BashOperator(
    task_id='test_spark_minio_integration',
    bash_command='''
    /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --py-files /dev/null \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=testuser \
        --conf spark.hadoop.fs.s3a.secret.key=password \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate(); df = spark.read.csv('s3a://bucket1/sample-data.csv', header=True); print(f'Read {df.count()} rows'); spark.stop()"
    ''',
    dag=dag,
)

# Set task dependencies
test_python_task >> test_minio_task >> test_spark_task >> test_integration_task