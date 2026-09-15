"""
DAG de produção: pipeline de ingestão, extração e validação dos preços
de combustíveis da ANP.

Etapas:
  1. scrape_anp_precos      -> baixa arquivos novos publicados pela ANP
  2. upload_bronze          -> envia os arquivos crus para o bucket
                               "bronze" do Minio (sem qualquer tratamento)
  3. extract_batch          -> lê os arquivos do bucket "bronze", repara
                               os malformados (fallback via LibreOffice),
                               extrai a aba MUNICIPIOS de cada um e salva
                               como .parquet no bucket "silver"
  4. validate_silver        -> gate de qualidade: valida schema, nulos,
                               consistência de preços e duplicatas no
                               silver consolidado. Se algo falhar, a
                               pipeline é interrompida aqui, antes de
                               propagar dado suspeito para o gold.
  5. trigger_anp_gold       -> dispara a DAG anp_gold (materialização e
                               registro das tabelas gold), só se todas
                               as etapas acima tiverem sido bem-sucedidas.

Todas as tasks de ingestão rodam no container auxiliar "py-toolbox" via
DockerOperator, disparado pelo Airflow através do socket do Docker do
host.

Agendamento: diário às 06h (a data de publicação da ANP varia entre
segunda e terça-feira, então rodar diariamente evita atraso de uma
semana inteira caso o dia varie). Isso significa que o gold também é
recalculado diariamente — se um dia isso se mostrar pesado demais para
o servidor, considere desacoplar a cadência do gold (ex: rodar
anp_gold em um schedule próprio, semanal, em vez de disparado a cada
execução deste pipeline).
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Caminho ABSOLUTO no HOST (não no container do Airflow) da pasta
# data/anp/ do projeto — necessário porque quem cria os containers
# py-toolbox é o Docker do HOST, via socket.
HOST_ANP_DIR = "/mnt/work/data-engineering/data/anp"

# Rede do projeto (necessária para resolver o hostname "minio" — a rede
# padrão do Docker não resolve nomes de containers de outros serviços).
PROJECT_NETWORK = "data-engineering_data-net"

MINIO_ENV = {
    "MINIO_ROOT_USER": os.environ.get("MINIO_ROOT_USER"),
    "MINIO_ROOT_PASSWORD": os.environ.get("MINIO_ROOT_PASSWORD"),
    "MINIO_ENDPOINT": "minio:9000",
}

with DAG(
    dag_id="anp_pipeline",
    description="Ingestão, extração e validação semanal dos preços de combustíveis da ANP",
    schedule="0 6 * * *",  # diário às 06h
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["anp", "ingestao", "bronze", "silver"],
) as dag:

    scrape = DockerOperator(
        task_id="scrape_anp_precos",
        image="py-toolbox",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",  # só precisa de acesso à internet
        command="scripts/scrape_anp_precos.py",
        mounts=[
            Mount(source=HOST_ANP_DIR, target="/app", type="bind"),
        ],
    )

    upload_bronze = DockerOperator(
        task_id="upload_bronze",
        image="py-toolbox",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode=PROJECT_NETWORK,  # precisa resolver o hostname "minio"
        command="scripts/anp_upload_bronze.py",
        environment=MINIO_ENV,
        mounts=[
            Mount(source=HOST_ANP_DIR, target="/app", type="bind"),
        ],
    )

    extract_batch = DockerOperator(
        task_id="extract_batch",
        image="py-toolbox",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode=PROJECT_NETWORK,  # precisa resolver o hostname "minio"
        command="scripts/anp_extract_batch.py",
        environment=MINIO_ENV,
        mounts=[
            Mount(source=HOST_ANP_DIR, target="/app", type="bind"),
        ],
    )

    validate_silver = DockerOperator(
        task_id="validate_silver",
        image="py-toolbox",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode=PROJECT_NETWORK,  # precisa resolver o hostname "minio"
        command="scripts/anp_validate_silver.py",
        environment=MINIO_ENV,
        mounts=[
            Mount(source=HOST_ANP_DIR, target="/app", type="bind"),
        ],
    )

    trigger_anp_gold = TriggerDagRunOperator(
        task_id="trigger_anp_gold",
        trigger_dag_id="anp_gold",
        wait_for_completion=False,  # não bloqueia anp_pipeline esperando o gold terminar
    )

    scrape >> upload_bronze >> extract_batch >> validate_silver >> trigger_anp_gold
