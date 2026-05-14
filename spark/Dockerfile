FROM apache/spark:3.5.0

USER root

ARG SPARK_JARS=/opt/spark/jars

# S3A jars — hadoop-aws version must match Hadoop 3.3.4 bundled with Spark 3.5.0
# aws-java-sdk-bundle version must be compatible with hadoop-aws-3.3.4
RUN curl -fsSL -o ${SPARK_JARS}/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o ${SPARK_JARS}/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    # Kafka structured streaming jars for Spark 3.5.0 + Scala 2.12
    && curl -fsSL -o ${SPARK_JARS}/spark-sql-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    && curl -fsSL -o ${SPARK_JARS}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    && curl -fsSL -o ${SPARK_JARS}/kafka-clients-3.4.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -fsSL -o ${SPARK_JARS}/commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

USER spark
