FROM apache/spark:3.5.0

USER root

ARG SPARK_JARS=/opt/spark/jars

# GCS connector — auth via GOOGLE_APPLICATION_CREDENTIALS env var (ADC)
RUN curl -fsSL -o ${SPARK_JARS}/gcs-connector-hadoop3-latest.jar \
        https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

USER spark
