"""
Silver -> Gold: lê os arquivos .parquet do silver (Minio) e materializa
como tabelas Delta no gold (fisicamente, em s3a://gold/warehouse/...).

IMPORTANTE — este script usa um metastore Hive local/isolado (o padrão
default do Spark, sem configuração de metastore compartilhado), assim
como bronze_to_silver.py e silver_to_gold.py do professor. Isso é
intencional: ele NÃO registra as tabelas no catálogo do Thrift Server
diretamente (o Thrift Server usa seu próprio metastore, em
/metastore/metastore_db — Derby não suporta múltiplas conexões
concorrentes de processos diferentes).

Depois de rodar este script, é necessário registrar cada tabela no
catálogo do Thrift Server via beeline (rodando do spark-master, que
conecta como cliente JDBC, sem abrir uma segunda conexão direta ao
Derby):

    docker exec -it spark-master beeline -u jdbc:hive2://spark-thrift-server:10000 \\
        -e "CREATE TABLE IF NOT EXISTS default.fct_precos_municipios USING DELTA LOCATION 's3a://gold/warehouse/fct_precos_municipios';"

    (repetir para cada uma das 7 tabelas geradas — ver lista abaixo)

Esse é o mesmo padrão documentado no README original do projeto
(seção "Passo 2 — Registrar a tabela no catálogo").

Tabelas geradas:
    default.fct_precos_municipios
    default.fct_precos_capitais
    default.fct_precos_estados
    default.fct_precos_regioes
    default.fct_precos_brasil
    default.dim_produto              (inclui volatilidade relativa / CV)
    default.fct_diferenca_capital_estado

Uso: roda de dentro do container airflow (tem PySpark e não conflita
com o metastore do Thrift Server):

    docker exec -it airflow python /app/anp_gold_build.py
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")

SILVER_BASE = "s3a://silver/anp"
GOLD_WAREHOUSE = "s3a://gold/warehouse"

NIVEIS_GEOGRAFICOS = ["municipios", "capitais", "estados", "regioes", "brasil"]

# Lista única de (nome_tabela, caminho_delta) — usada tanto por este
# script quanto pela DAG anp_gold (que a importa para o passo de
# registro via beeline), evitando manter a lista duplicada em dois
# arquivos.
GOLD_TABLES = [
    (f"default.fct_precos_{nivel}", f"{GOLD_WAREHOUSE}/fct_precos_{nivel}")
    for nivel in NIVEIS_GEOGRAFICOS
] + [
    ("default.dim_produto", f"{GOLD_WAREHOUSE}/dim_produto"),
    ("default.fct_diferenca_capital_estado", f"{GOLD_WAREHOUSE}/fct_diferenca_capital_estado"),
]

# Mapa de acentos para remoção simples (evita depender de bibliotecas
# externas de normalização Unicode dentro do driver Spark).
_ACENTOS = str.maketrans("ÁÃÂÀÉÊÍÓÕÔÚÇ", "AAAAEEIOOOUC")


def normalizar_nomes_colunas(df):
    """
    Renomeia colunas para o padrão MAIUSCULO_COM_UNDERSCORE, sem espaços
    nem acentos — obrigatório para o Delta Lake (que não aceita espaços
    ou caracteres especiais em nomes de coluna sem habilitar Column
    Mapping) e mais prático de referenciar depois em SQL/Superset.
    """
    for coluna in df.columns:
        novo_nome = coluna.strip().upper().translate(_ACENTOS).replace(" ", "_")
        if novo_nome != coluna:
            df = df.withColumnRenamed(coluna, novo_nome)
    return df


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("anp_gold_build")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", ",".join([
            "io.delta:delta-core_2.12:2.4.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", GOLD_WAREHOUSE)
        .enableHiveSupport()
        .getOrCreate()
    )


def materializar_niveis_geograficos(spark: SparkSession) -> dict:
    """
    Lê cada nível geográfico do silver e materializa como tabela Delta
    no gold, registrada no metastore. Retorna os DataFrames já lidos,
    para reuso nas tabelas derivadas (evita reler do Minio).
    """
    dataframes = {}

    for nivel in NIVEIS_GEOGRAFICOS:
        print(f"Processando nível: {nivel}")
        df = spark.read.parquet(f"{SILVER_BASE}/{nivel}/")
        df = normalizar_nomes_colunas(df)

        nome_tabela = f"default.fct_precos_{nivel}"
        caminho_gold = f"{GOLD_WAREHOUSE}/fct_precos_{nivel}"

        df.write.format("delta").mode("overwrite") \
            .option("path", caminho_gold) \
            .option("mergeSchema", "true") \
            .saveAsTable(nome_tabela)

        print(f"  -> {nome_tabela} ({df.count()} linhas)")
        dataframes[nivel] = df

    return dataframes


def materializar_dim_produto(spark: SparkSession, df_brasil) -> None:
    """
    Calcula a volatilidade relativa (coeficiente de variação) de cada
    produto ao longo do tempo, usando a série nacional (BRASIL) como
    referência, e materializa como dimensão no gold.
    """
    print("Processando dim_produto")

    dim_produto = (
        df_brasil.groupBy("PRODUTO")
        .agg(
            F.avg("PRECO_MEDIO_REVENDA").alias("preco_medio_historico"),
            F.stddev("PRECO_MEDIO_REVENDA").alias("desvio_padrao_historico"),
            F.first("UNIDADE_DE_MEDIDA").alias("unidade_medida"),
        )
        .withColumn(
            "volatilidade_relativa",
            F.col("desvio_padrao_historico") / F.col("preco_medio_historico"),
        )
    )

    nome_tabela = "default.dim_produto"
    caminho_gold = f"{GOLD_WAREHOUSE}/dim_produto"

    dim_produto.write.format("delta").mode("overwrite") \
        .option("path", caminho_gold) \
        .option("mergeSchema", "true") \
        .saveAsTable(nome_tabela)

    print(f"  -> {nome_tabela} ({dim_produto.count()} linhas)")


def materializar_diferenca_capital_estado(spark: SparkSession, df_capitais, df_estados) -> None:
    """
    Calcula, semana a semana, a diferença de preço entre a capital de
    cada estado e a média do estado inteiro — achado relevante da
    exploração KDD (em vários estados o interior é mais caro que a
    capital, provavelmente por custo logístico).
    """
    print("Processando fct_diferenca_capital_estado")

    capitais_renomeado = (
        df_capitais
        .select(
            "DATA_INICIAL", "ESTADO", "PRODUTO",
            F.col("PRECO_MEDIO_REVENDA").alias("preco_capital"),
        )
    )

    estados_renomeado = (
        df_estados
        .select(
            "DATA_INICIAL",
            F.col("ESTADOS").alias("ESTADO"),
            "PRODUTO",
            F.col("PRECO_MEDIO_REVENDA").alias("preco_estado"),
        )
    )

    diferenca = (
        capitais_renomeado.join(
            estados_renomeado,
            on=["DATA_INICIAL", "ESTADO", "PRODUTO"],
            how="inner",
        )
        .withColumn("diferenca_capital_estado", F.col("preco_capital") - F.col("preco_estado"))
    )

    nome_tabela = "default.fct_diferenca_capital_estado"
    caminho_gold = f"{GOLD_WAREHOUSE}/fct_diferenca_capital_estado"

    diferenca.write.format("delta").mode("overwrite") \
        .option("path", caminho_gold) \
        .option("mergeSchema", "true") \
        .saveAsTable(nome_tabela)

    print(f"  -> {nome_tabela} ({diferenca.count()} linhas)")


def run():
    spark = get_spark_session()

    dataframes = materializar_niveis_geograficos(spark)
    materializar_dim_produto(spark, dataframes["brasil"])
    materializar_diferenca_capital_estado(spark, dataframes["capitais"], dataframes["estados"])

    spark.stop()
    print("\nConcluído: todas as tabelas gold materializadas.")


if __name__ == "__main__":
    run()
