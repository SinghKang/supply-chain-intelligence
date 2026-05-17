FROM apache/airflow:2.8.1

USER root

# Install Java — required for PySpark
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    kafka-python \
    minio \
    psycopg2-binary \
    pyspark==3.5.0