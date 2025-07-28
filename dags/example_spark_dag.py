from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'example_spark_submit',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

spark_submit = BashOperator(
    task_id='spark_pi',
    bash_command='spark-submit --master local[*] --class org.apache.spark.examples.SparkPi opt/bitnami/spark/examples/jars/spark-examples_2.12-3.5.6.jar 10',
    dag=dag,
)

