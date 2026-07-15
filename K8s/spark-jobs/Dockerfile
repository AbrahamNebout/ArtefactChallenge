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

COPY . .

ENV SPARK_CONF_DIR=/app/conf

ENTRYPOINT ["/opt/spark/kubernetes/dockerfiles/spark/entrypoint.sh"]