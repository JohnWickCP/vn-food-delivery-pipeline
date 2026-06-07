FROM apache/spark:3.5.0

USER root

ARG SPARK_JARS=/opt/spark/jars

# GCS connector — replaces hadoop-aws + aws-java-sdk-bundle
# Auth via GOOGLE_APPLICATION_CREDENTIALS env var (ADC)
RUN curl -fsSL -o ${SPARK_JARS}/gcs-connector-hadoop3-latest.jar \
        https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    # Kafka structured streaming jars for Spark 3.5.0 + Scala 2.12
    && curl -fsSL -o ${SPARK_JARS}/spark-sql-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    && curl -fsSL -o ${SPARK_JARS}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    && curl -fsSL -o ${SPARK_JARS}/kafka-clients-3.4.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -fsSL -o ${SPARK_JARS}/commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Python packages for Avro deserialization via Python UDF
RUN pip install --no-cache-dir fastavro==1.9.4

USER spark
