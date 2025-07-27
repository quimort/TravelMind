FROM apache/airflow:2.10.4-python3.11

USER root

# Install Java and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    gcc \
    python3-dev \
    curl \
    wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Download and install Spark
RUN mkdir -p ${SPARK_HOME} && \
    curl -L https://archive.apache.org/dist/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz \
    -o spark-3.5.6-bin-hadoop3.tgz && \
    tar xvzf spark-3.5.6-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 && \
    rm -rf spark-3.5.6-bin-hadoop3.tgz

# Download AWS dependencies for MinIO/S3 support
RUN wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Create Spark configuration
RUN mkdir -p ${SPARK_HOME}/conf
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# Create events directory and set permissions
RUN mkdir -p /opt/spark/events && \
    chown -R airflow:root /opt/spark && \
    chmod -R 775 /opt/spark

USER airflow

# Install Airflow providers and Spark dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    pyspark==3.5.6

# Copy configuration files
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf