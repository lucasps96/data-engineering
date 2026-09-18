FROM apache/superset:GHA-dev-16080735049

USER root

# Install system dependencies required for SASL and Thrift
RUN apt-get update && apt-get install -y \
    build-essential \
    libsasl2-dev \
    libssl-dev \
    libkrb5-dev \
    python3-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

RUN /app/.venv/bin/python3 -m ensurepip --upgrade

# Install Python packages required for Hive/Spark Thrift
RUN /app/.venv/bin/python3 -m pip install --no-cache-dir \
    thrift==0.16.0 \
    thrift-sasl==0.4.3 \
    sasl==0.3.1 \
    pyhive

USER superset



