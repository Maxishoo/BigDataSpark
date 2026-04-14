FROM apache/spark:3.4.1-python3

USER root
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    psycopg2-binary \
    clickhouse-driver \
    pandas

WORKDIR /opt/spark/work-dir

COPY jars/*.jar /opt/spark/jars/

USER spark

CMD ["bash"]