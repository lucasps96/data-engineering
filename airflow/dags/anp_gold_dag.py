"""
DAG anp_gold: materializa as tabelas gold (preços ANP) e as registra
no catálogo do Thrift Server.

Pressupõe que o silver já foi validado pela DAG anp_pipeline (schedule=None
aqui — dispare manualmente ou encadeie via TriggerDagRunOperator/
ExternalTaskSensor a partir de anp_pipeline, se quiser automação completa).

Etapas:
  1. build_gold      -> roda anp_gold_build.py (mesmo script já usado
                        manualmente, sem duplicação: chamado via
                        BashOperator, arquivo único em spark/)
  2. register_tables -> registra cada tabela no catálogo do Thrift
                        Server via beeline, rodando dentro do container
                        spark-master (necessário porque o Thrift Server
                        usa seu próprio metastore — ver docstring de
                        anp_gold_build.py para o porquê)

A lista de tabelas (GOLD_TABLES) é importada diretamente de
anp_gold_build.py, não duplicada aqui.
"""
import os
import sys
from datetime import datetime, timedelta

import docker
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# spark/ está montado em /app dentro do container airflow (ver
# docker-compose.yml) — adicionamos ao path para importar a lista de
# tabelas sem duplicá-la.
sys.path.append("/app")
from anp_gold_build import GOLD_TABLES  # noqa: E402

THRIFT_JDBC_URL = "jdbc:hive2://spark-thrift-server:10000"
SPARK_MASTER_CONTAINER = "spark-master"


def registrar_tabelas_no_catalogo():
    """
    Registra cada tabela gold no catálogo do Thrift Server via beeline,
    executado dentro do container spark-master (que já tem o cliente
    beeline instalado) através do socket do Docker do host.
    """
    client = docker.from_env()
    container = client.containers.get(SPARK_MASTER_CONTAINER)

    for nome_tabela, caminho in GOLD_TABLES:
        comando_sql = (
            f"CREATE TABLE IF NOT EXISTS {nome_tabela} "
            f"USING DELTA LOCATION '{caminho}';"
        )
        exit_code, output = container.exec_run(
            ["beeline", "-u", THRIFT_JDBC_URL, "-e", comando_sql]
        )
        saida = output.decode(errors="replace")
        print(f"[{nome_tabela}] exit_code={exit_code}\n{saida}")

        if exit_code != 0:
            raise RuntimeError(f"Falha ao registrar {nome_tabela}:\n{saida}")


# retries=0: este job é pesado para o hardware disponível (~7,6GB RAM
# compartilhados com todos os outros serviços) — uma reexecução
# automática após falha só agravaria uma eventual pressão de memória
# já existente, em vez de ajudar. Prefira disparar manualmente de novo
# depois de confirmar que há memória livre (free -h).
default_args = {
    "retries": 0,
}

with DAG(
    dag_id="anp_gold",
    description="Materializa e registra as tabelas gold dos preços de combustíveis (ANP)",
    schedule=None,  # dispare manualmente, ou encadeie a partir de anp_pipeline
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["anp", "gold"],
) as dag:

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command="python /app/anp_gold_build.py",
        execution_timeout=timedelta(minutes=20),
    )

    register_tables = PythonOperator(
        task_id="register_tables",
        python_callable=registrar_tabelas_no_catalogo,
        execution_timeout=timedelta(minutes=5),
    )

    build_gold >> register_tables
