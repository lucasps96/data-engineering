"""
DAG de teste: valida que o Airflow consegue disparar o container
py-toolbox via DockerOperator, rodando o script de extração (anp_extract.py)
contra um único arquivo conhecido.

Esta DAG é só para validação da integração — não faz parte do pipeline
de produção ainda. Depois de confirmar que funciona, a lógica real vai
para a DAG "anp_pipeline" (scraping -> bronze -> extração -> silver -> gold).

Trigger manual (não tem agendamento automático): pela UI do Airflow,
clique no botão de "play" ao lado do nome da DAG, ou via CLI:
    docker exec -it airflow airflow dags trigger anp_extract_test
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Caminho ABSOLUTO no HOST (não no container do Airflow) da pasta
# data/anp/ do projeto. O DockerOperator precisa do caminho real do host,
# porque quem cria o container py-toolbox é o Docker do HOST (via socket),
# não o container do Airflow.
HOST_ANP_DIR = "/mnt/work/data-engineering/data/anp"

with DAG(
    dag_id="anp_extract_test",
    description="Teste de integração: Airflow -> DockerOperator -> py-toolbox",
    schedule=None,  # só trigger manual, não é a DAG de produção ainda
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["teste", "anp", "docker-operator"],
) as dag:

    testar_extracao = DockerOperator(
        task_id="testar_extracao_py_toolbox",
        image="py-toolbox",
        api_version="auto",
        auto_remove=True,  # equivalente ao --rm do docker-run.sh
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        # O entrypoint da imagem já é ["python"], então "command" aqui
        # equivale aos argumentos que viriam depois de "python" na linha
        # de comando (mesma lógica do docker-run.sh).
        command="scripts/anp_extract.py --arquivo dados/resumo_semanal_lpc_2024-12-01_2024-12-07.xlsx",
        mounts=[
            Mount(source=HOST_ANP_DIR, target="/app", type="bind"),
        ],
    )
