FROM eclipse-temurin:17-jdk-jammy

ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON=python3

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip curl wget procps net-tools \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Installer Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Télécharger les JARs Delta Lake + Hadoop AWS directement dans Spark
RUN wget -q -P ${SPARK_HOME}/jars/ \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar

RUN pip3 install --no-cache-dir \
    pyspark==3.5.0 delta-spark==3.2.0 boto3==1.34.0 \
    pyarrow==15.0.2 pandas==2.2.2

RUN mkdir -p /opt/spark/logs /opt/spark/work
WORKDIR $SPARK_HOME
