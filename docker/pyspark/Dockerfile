FROM python:3.11-slim

# Java para o Spark
RUN apt-get update && apt-get install -y openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark pyarrow psutil

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
WORKDIR /app
CMD ["bash"]
