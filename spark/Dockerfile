FROM apache/spark:3.5.0

USER root

# Cài Python deps cho PySpark jobs
RUN pip install --no-cache-dir \
    boto3 \
    kafka-python \
    great-expectations \
    pandas

RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark/.ivy2
ARG JAR_DIR=/opt/spark/jars

# Nessie version must be defined before downloading the Nessie JAR
ARG NESSIE_VERSION=0.104.5

# Download JARs cần thiết (Iceberg + S3 + Kafka)
ARG JAR_DIR=/opt/spark/jars
RUN curl -o $JAR_DIR/iceberg-spark-runtime.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar && \
    curl -o $JAR_DIR/hadoop-aws.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o $JAR_DIR/aws-java-sdk-bundle.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -o $JAR_DIR/spark-sql-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -o $JAR_DIR/kafka-clients.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    curl -o $JAR_DIR/spark-token-provider-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -o $JAR_DIR/commons-pool2.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -o $JAR_DIR/nessie-spark-extensions.jar \
    https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar && \
    curl -o $JAR_DIR/iceberg-nessie.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-nessie/1.4.2/iceberg-nessie-1.4.2.jar

# Copy spark-defaults.conf (cấu hình S3/Iceberg mặc định)
COPY spark/config/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
WORKDIR /opt/spark-jobs
ENV PYTHONPATH=/opt/spark-jobs
ENV NESSIE_VERSION=0.104.5

USER spark