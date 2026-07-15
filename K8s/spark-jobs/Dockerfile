FROM eclipse-temurin:17-jre-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip curl tini \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s /usr/bin/python3 /usr/bin/python

ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    -o /tmp/spark.tgz \
 && mkdir -p ${SPARK_HOME} \
 && tar -xzf /tmp/spark.tgz -C ${SPARK_HOME} --strip-components=1 \
 && rm /tmp/spark.tgz

ENV PATH="${SPARK_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

RUN mkdir -p /tmp/extra-jars && cd /tmp/extra-jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.0/iceberg-spark-runtime-3.5_2.12-1.6.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.0/iceberg-aws-bundle-1.6.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    cp *.jar ${SPARK_HOME}/jars/ && \
    cd / && rm -rf /tmp/extra-jars


COPY . .

ENV SPARK_CONF_DIR=/app/conf

ENTRYPOINT ["/opt/spark/kubernetes/dockerfiles/spark/entrypoint.sh"]