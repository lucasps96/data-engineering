"""
Sobe os arquivos .xlsx já baixados (bronze local, em dados/) para o
bucket "bronze" do Minio, sob o prefixo "anp/resumo_semanal/".

Os arquivos são enviados exatamente como estão — sem nenhum tratamento —
respeitando o princípio de bronze (cópia fiel da fonte).

Variáveis de ambiente esperadas (mesmas já usadas no restante do
projeto, via .env):
    MINIO_ENDPOINT       (default: "minio:9000")
    MINIO_ROOT_USER
    MINIO_ROOT_PASSWORD
    MINIO_BUCKET         (default: "bronze")
    MINIO_PREFIX         (default: "anp/resumo_semanal/")

Uso (de dentro de data/anp/, via toolbox, com acesso à rede do projeto):
    docker run --rm -it \\
        --network data-engineering_data-net \\
        --env-file /mnt/work/data-engineering/.env \\
        -v "$(pwd)":/app \\
        py-toolbox scripts/anp_upload_bronze.py

    # Para reenviar mesmo os que já existem no bucket:
    ... scripts/anp_upload_bronze.py --forcar
"""

import argparse
import os
import sys

from minio import Minio
from minio.error import S3Error

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DADOS_DIR = os.path.join(SCRIPT_DIR, "..", "dados")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "bronze")
MINIO_PREFIX = os.environ.get("MINIO_PREFIX", "anp/resumo_semanal/")


def get_client() -> Minio:
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        print(
            "ERRO: MINIO_ROOT_USER / MINIO_ROOT_PASSWORD não encontrados nas "
            "variáveis de ambiente. Rode com --env-file apontando para o .env "
            "do projeto.",
            file=sys.stderr,
        )
        sys.exit(1)

    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,  # rede interna do Docker, sem TLS
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--forcar", action="store_true",
        help="Reenvia mesmo arquivos que já existem no bucket (default: pula)"
    )
    args = parser.parse_args()

    client = get_client()

    if not client.bucket_exists(MINIO_BUCKET):
        print(f"Bucket '{MINIO_BUCKET}' não existe. Criando...")
        client.make_bucket(MINIO_BUCKET)

    files = sorted(f for f in os.listdir(DADOS_DIR) if f.lower().endswith(".xlsx"))
    print(f"Encontrados {len(files)} arquivos locais em {DADOS_DIR}\n")

    enviados = []
    pulados = []
    erros = []

    for i, filename in enumerate(files, 1):
        local_path = os.path.join(DADOS_DIR, filename)
        object_name = MINIO_PREFIX.rstrip("/") + "/" + filename

        if not args.forcar:
            try:
                client.stat_object(MINIO_BUCKET, object_name)
                pulados.append(filename)
                print(f"[{i}/{len(files)}] {filename} ... já existe no bucket, pulando")
                continue
            except S3Error as e:
                if e.code != "NoSuchKey":
                    erros.append((filename, str(e)))
                    print(f"[{i}/{len(files)}] {filename} ... ERRO ao checar: {e}")
                    continue
                # NoSuchKey = ainda não existe, segue para o upload

        print(f"[{i}/{len(files)}] {filename} ... ", end="", flush=True)
        try:
            client.fput_object(MINIO_BUCKET, object_name, local_path)
            enviados.append(filename)
            print("OK")
        except S3Error as e:
            erros.append((filename, str(e)))
            print(f"ERRO: {e}")

    print("\n" + "=" * 70)
    print("RESUMO DO UPLOAD")
    print("=" * 70)
    print(f"Total de arquivos locais: {len(files)}")
    print(f"Enviados com sucesso:     {len(enviados)}")
    print(f"Já existiam (pulados):    {len(pulados)}")
    print(f"Falharam:                 {len(erros)}")

    if erros:
        print("\nArquivos com erro:")
        for filename, err in erros:
            print(f"  - {filename}: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
