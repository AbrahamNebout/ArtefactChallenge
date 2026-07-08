FROM eclipse-temurin:17-jre-jammy

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

# spark-submit lira automatiquement les configs par défaut ici
ENV SPARK_CONF_DIR=/app/conf

CMD ["tail", "-f", "/dev/null"]