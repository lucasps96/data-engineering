"""
Processa em lote os arquivos "resumo semanal" (.xlsx) do bucket BRONZE
do Minio, extraindo TODAS as 5 abas (MUNICIPIOS, ESTADOS, REGIOES,
CAPITAIS, BRASIL) de cada um para .parquet no bucket SILVER, cada aba
em seu próprio prefixo (ex.: anp/municipios/, anp/estados/, ...).

O disco local é usado só como espaço de trabalho transitório (download
-> processa -> upload -> limpa), sem depender de nenhuma pasta
persistente entre execuções.

Reaproveita a lógica de extração/reparo já validada em anp_extract.py
(extract_sheet).

Variáveis de ambiente esperadas (mesmas do restante do projeto):
    MINIO_ENDPOINT        (default: "minio:9000")
    MINIO_ROOT_USER
    MINIO_ROOT_PASSWORD
    MINIO_BRONZE_BUCKET   (default: "bronze")
    MINIO_BRONZE_PREFIX   (default: "anp/resumo_semanal/")
    MINIO_SILVER_BUCKET   (default: "silver")
    MINIO_SILVER_PREFIX_BASE (default: "anp/") — cada aba vira um
        subprefixo: anp/municipios/, anp/estados/, anp/regioes/,
        anp/capitais/, anp/brasil/

Uso (precisa estar na rede do projeto, para resolver o hostname "minio"):
    docker run --rm -it \\
        --network data-engineering_data-net \\
        --env-file /mnt/work/data-engineering/.env \\
        -v "$(pwd)":/app \\
        py-toolbox scripts/anp_extract_batch.py

    # Para processar só uma aba específica:
    ... scripts/anp_extract_batch.py --aba MUNICIPIOS

    # Para reprocessar mesmo os que já têm .parquet no silver:
    ... scripts/anp_extract_batch.py --forcar
"""

import argparse
import os
import sys
import tempfile
import time

from minio import Minio
from minio.error import S3Error

# Reaproveita a lógica já testada de extração/reparo (mesma pasta)
from anp_extract import extract_sheet, SHEETS_DISPONIVEIS

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")

BRONZE_BUCKET = os.environ.get("MINIO_BRONZE_BUCKET", "bronze")
BRONZE_PREFIX = os.environ.get("MINIO_BRONZE_PREFIX", "anp/resumo_semanal/")
SILVER_BUCKET = os.environ.get("MINIO_SILVER_BUCKET", "silver")
SILVER_PREFIX_BASE = os.environ.get("MINIO_SILVER_PREFIX_BASE", "anp/")


def get_client() -> Minio:
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        print(
            "ERRO: MINIO_ROOT_USER / MINIO_ROOT_PASSWORD não encontrados nas "
            "variáveis de ambiente.",
            file=sys.stderr,
        )
        sys.exit(1)
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def object_exists(client: Minio, bucket: str, object_name: str) -> bool:
    try:
        client.stat_object(bucket, object_name)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        raise


def silver_prefix_para_aba(aba: str) -> str:
    return SILVER_PREFIX_BASE.rstrip("/") + "/" + aba.lower() + "/"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--aba", choices=SHEETS_DISPONIVEIS, default=None,
        help="Processa só esta aba (default: todas as 5)"
    )
    parser.add_argument(
        "--forcar", action="store_true",
        help="Reprocessa mesmo arquivos que já têm .parquet no silver (default: pula)"
    )
    args = parser.parse_args()

    abas_a_processar = [args.aba] if args.aba else SHEETS_DISPONIVEIS

    client = get_client()

    if not client.bucket_exists(SILVER_BUCKET):
        print(f"Bucket '{SILVER_BUCKET}' não existe. Criando...")
        client.make_bucket(SILVER_BUCKET)

    objetos_bronze = [
        obj.object_name
        for obj in client.list_objects(BRONZE_BUCKET, prefix=BRONZE_PREFIX, recursive=True)
        if obj.object_name.lower().endswith(".xlsx")
    ]
    objetos_bronze.sort()
    print(f"Encontrados {len(objetos_bronze)} arquivos em {BRONZE_BUCKET}/{BRONZE_PREFIX}")
    print(f"Abas a processar: {abas_a_processar}\n")

    resumo_por_aba = {}
    inicio_total = time.time()

    for aba in abas_a_processar:
        silver_prefix = silver_prefix_para_aba(aba)
        print(f"\n{'='*70}\nAba: {aba}  ->  {SILVER_BUCKET}/{silver_prefix}\n{'='*70}")

        sucesso, fallback, pulados, erros = [], [], [], []
        inicio_aba = time.time()

        for i, object_name in enumerate(objetos_bronze, 1):
            filename = os.path.basename(object_name)
            base_name = os.path.splitext(filename)[0]
            silver_object_name = silver_prefix + base_name + ".parquet"

            if not args.forcar and object_exists(client, SILVER_BUCKET, silver_object_name):
                pulados.append(filename)
                print(f"[{i}/{len(objetos_bronze)}] {filename} ... já existe, pulando")
                continue

            print(f"[{i}/{len(objetos_bronze)}] {filename} ... ", end="", flush=True)

            fd_xlsx, tmp_xlsx_path = tempfile.mkstemp(suffix=".xlsx")
            os.close(fd_xlsx)
            fd_parquet, tmp_parquet_path = tempfile.mkstemp(suffix=".parquet")
            os.close(fd_parquet)

            try:
                client.fget_object(BRONZE_BUCKET, object_name, tmp_xlsx_path)

                df, used_fallback = extract_sheet(tmp_xlsx_path, aba)
                df["ARQUIVO_ORIGEM"] = filename
                # coerce_timestamps="us": evita "Illegal Parquet type:
                # INT64 (TIMESTAMP(NANOS,false))" ao ler pelo Spark.
                df.to_parquet(tmp_parquet_path, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)

                client.fput_object(SILVER_BUCKET, silver_object_name, tmp_parquet_path)

                sucesso.append(filename)
                if used_fallback:
                    fallback.append(filename)
                    print(f"OK ({len(df)} linhas, via fallback LibreOffice)")
                else:
                    print(f"OK ({len(df)} linhas)")
            except Exception as e:
                erros.append((filename, str(e)))
                print(f"ERRO: {e}")
            finally:
                for p in (tmp_xlsx_path, tmp_parquet_path):
                    if os.path.exists(p):
                        os.remove(p)

        duracao_aba = time.time() - inicio_aba
        resumo_por_aba[aba] = {
            "sucesso": len(sucesso),
            "fallback": len(fallback),
            "pulados": len(pulados),
            "erros": erros,
            "duracao": duracao_aba,
        }

    duracao_total = time.time() - inicio_total

    print("\n" + "=" * 70)
    print("RESUMO GERAL (bronze -> silver, todas as abas)")
    print("=" * 70)

    houve_erro = False
    for aba, resumo in resumo_por_aba.items():
        print(f"\n{aba}:")
        print(f"  Processados com sucesso: {resumo['sucesso']}")
        print(f"  (via fallback LibreOffice): {resumo['fallback']}")
        print(f"  Já existiam: {resumo['pulados']}")
        print(f"  Falharam: {len(resumo['erros'])}")
        print(f"  Tempo: {resumo['duracao']:.1f}s")
        if resumo["erros"]:
            houve_erro = True
            for filename, err in resumo["erros"]:
                print(f"    - {filename}: {err}")

    print(f"\nTempo total: {duracao_total:.1f}s")

    if houve_erro:
        sys.exit(1)


if __name__ == "__main__":
    main()
