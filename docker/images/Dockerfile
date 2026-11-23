# Spark 3.5.9 / Java 17 / Python 3 batch image with the Kafka connector baked in (no --packages at runtime).
FROM apache/spark:3.5.9-java17-python3
USER root
WORKDIR /opt/project

# Kafka source for Spark 3.5.9 (Scala 2.12) and its runtime dependencies, pinned.
ARG MAVEN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9/spark-token-provider-kafka-0-10_2.12-3.5.9.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip3 install --no-cache-dir 'PyYAML>=6.0,<7' 'pytest>=8,<10'
COPY configs ./configs
COPY reference ./reference
COPY contracts ./contracts
COPY docs ./docs
COPY tests ./tests
COPY scripts ./scripts
ENV PYTHONPATH=/opt/project/src
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
USER 185
