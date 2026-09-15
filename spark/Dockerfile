# Note: the -alpine variants of eclipse-temurin are published for amd64 only,
# so they do not build on Apple Silicon. The -jammy variants are multi-arch.
# Java 17 (não 11): precisa bater com a versão usada pelos spark-worker-*,
# senão a serialização de lambdas entre driver e executores quebra com
# "ClassCastException: cannot assign instance of SerializedLambda".
FROM eclipse-temurin:17-jre-jammy

RUN apt-get update && apt-get install -y --no-install-recommends wget python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /spark

ENV SPARK_VERSION 3.4.3
ENV HADOOP_VERSION 3

ENV SPARK_ARCHIVE spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN wget "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_ARCHIVE}"
RUN tar -xf $SPARK_ARCHIVE
RUN rm $SPARK_ARCHIVE

ENV SPARK_DIR "/spark/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"

# Necessário para rodar scripts .py via spark-submit (ex.: anp_gold_build.py):
# o pacote Spark já inclui o PySpark e o Py4J internamente em python/,
# só precisa do interpretador Python e do PYTHONPATH apontando pra lá.
ENV PYTHONPATH "${SPARK_DIR}/python:${SPARK_DIR}/python/lib/py4j-0.10.9.7-src.zip"
ENV PYSPARK_PYTHON python3

# Baixar JARs necessários para S3A + Delta Lake
RUN wget -P ${SPARK_DIR}/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P ${SPARK_DIR}/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -P ${SPARK_DIR}/jars https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    wget -P ${SPARK_DIR}/jars https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

EXPOSE 10001
EXPOSE 10000

# Os --add-opens abaixo são necessários no Java 17 (não eram no Java 11):
# o module system do JDK 9+ bloqueia acesso a APIs internas (ex.:
# sun.nio.ch.DirectBuffer) que o Spark usa para gerenciamento de memória
# off-heap, causando "IllegalAccessError" sem essas flags.
ENTRYPOINT java \
 --add-opens=java.base/java.lang=ALL-UNNAMED \
 --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
 --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
 --add-opens=java.base/java.io=ALL-UNNAMED \
 --add-opens=java.base/java.net=ALL-UNNAMED \
 --add-opens=java.base/java.nio=ALL-UNNAMED \
 --add-opens=java.base/java.util=ALL-UNNAMED \
 --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
 --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
 --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
 --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
 --add-opens=java.base/sun.security.action=ALL-UNNAMED \
 --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
 -Duser.timezone=Etc/UTC \
 -Xmx512m \
 -cp "${SPARK_DIR}/conf:${SPARK_DIR}/jars/*" \
 org.apache.spark.deploy.SparkSubmit \
 --master spark://spark-master:7077 \
 --conf spark.executor.extraJavaOptions=-Duser.timezone=Etc/UTC \
 --conf spark.eventLog.enabled=false \
 --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 \
 --name "Thrift JDBC/ODBC Server" \
 --executor-memory 512m \
 --executor-cores 1 \
 --driver-memory 512m \
 --conf spark.cores.max=1 \
 --conf spark.executor.instances=1 \
 --conf spark.sql.catalogImplementation=hive \
 --conf spark.hadoop.javax.jdo.option.ConnectionURL="jdbc:derby:;databaseName=/metastore/metastore_db;create=true" \
 --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
 --conf spark.hadoop.fs.s3a.access.key=minio \
 --conf spark.hadoop.fs.s3a.secret.key=minio123 \
 --conf spark.hadoop.fs.s3a.path.style.access=true \
 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
 --conf spark.sql.warehouse.dir=s3a://gold/warehouse \
 --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
 --conf spark.jars.packages=io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
 --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore \
 spark-internal