FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable (fixed for x86_64)
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Only install additional providers, not airflow itself
RUN pip install apache-airflow-providers-apache-spark pyspark