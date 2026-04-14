# ZMIANA: Używamy konkretnej wersji 3.5.1 (stabilnej dla Delty)
FROM apache/spark:3.5.1

USER root

# Instalacja narzędzi
RUN apt-get update && \
    apt-get install -y python3-pip curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Instalacja pakietów Python
RUN pip3 install --no-cache-dir \
    boto3 \
    pandas \
    pyarrow \
    numpy \
    delta-spark==3.1.0

# Pobieranie JAR-ów do AWS (poprawna ścieżka dla obrazu apache/spark)
RUN cd /opt/spark/jars && \
    curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Katalog na skrypty
RUN mkdir -p /opt/spark-jobs && \
    chmod 777 /opt/spark-jobs

WORKDIR /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

USER spark