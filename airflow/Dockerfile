FROM apache/airflow:2.9.0

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.2.0 \
    boto3==1.34.0 \
    minio==7.2.0 \
    pyarrow==15.0.2 \
    pandas==2.2.2 \
    cryptography==41.0.7
