FROM apache/airflow:2.9.1

# Install Java
USER root

# Add APT sources and install Java (git is needed by `dbt deps`)
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    git \
    && apt-get clean

# Set JAVA_HOME for PySpark. Resolve it from the installed java binary instead of
# hardcoding the path: Debian suffixes it with the architecture, so a hardcoded
# ".../java-1.17.0-openjdk-amd64" does not exist on arm64 (Apple Silicon) and
# PySpark fails with "Java gateway process exited before sending its port number".
RUN ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/current
ENV JAVA_HOME=/usr/lib/jvm/current
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install Trino CLI
RUN curl -L https://repo1.maven.org/maven2/io/trino/trino-cli/440/trino-cli-440-executable.jar -o /usr/local/bin/trino && \
    chmod +x /usr/local/bin/trino

USER airflow

# Install Python dependencies: PySpark, DBT
RUN pip install --no-cache-dir \
    pyspark==3.4.3 \
    dbt-core==1.10.9 \
    dbt-spark==1.9.3 \
    dbt-spark[PyHive] \
    apache-airflow-providers-docker

RUN pip install psycopg2-binary
