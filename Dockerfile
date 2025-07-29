FROM python:3.11-bullseye
# default shell is sh
RUN apt-get update
RUN apt-get install -y --no-install-recommends openjdk-17-jdk
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
ENV SPARK_HOME="/home/sparkuser/spark"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
RUN mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

# If it breaks in this step go to https://dlcdn.apache.org/spark/ and choose higher spark version instead
RUN curl https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz -o spark-3.5.6-bin-hadoop3.tgz \
    && tar xvzf spark-3.5.6-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf spark-3.5.6-bin-hadoop3.tgz
# Port master will be exposed
ENV SPARK_MASTER_PORT="7077"
# Name of master container and also counts as hostname
ENV SPARK_MASTER_HOST="spark-master"

# Install sbt using coursier (cs) to build jars (Comment these out if not needed to speed up image build)
# Replace the Coursier download section with the correct URLs:
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
        curl -fL https://github.com/VirtusLab/coursier-m1/releases/latest/download/cs-aarch64-pc-linux.gz | gzip -d > cs; \
    else \
        curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs; \
    fi && \
    chmod +x cs
ENV PATH="$PATH:/root/.local/share/coursier/bin"
# RUN ./cs setup  # Keep this commented out to avoid Rosetta issues

# Download postgres jar and add it to spark jars
RUN wget -P ${SPARK_HOME}/jars/ https://jdbc.postgresql.org/download/postgresql-42.7.4.jar;

# Download aws jar and add it to spark jars
RUN wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar;
RUN wget -P ${SPARK_HOME}/jars/ https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar;


RUN useradd -u 1000 -m -d /home/sparkuser sparkuser
ENV HOME="/home/sparkuser"
RUN chown -R 1000:1000 ${SPARK_HOME}
USER sparkuser

COPY ./spark-defaults.conf "${SPARK_HOME}/conf"

ENTRYPOINT ["/bin/bash"]