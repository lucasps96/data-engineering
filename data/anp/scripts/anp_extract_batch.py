"""
Processa em lote todos os arquivos "resumo semanal" (.xlsx) da pasta
dados/ (bronze), extraindo a aba MUNICIPIOS de cada um para um .parquet
correspondente em parquet/ (silver intermediário).

Reaproveita a lógica já validada em anp_extract.py (extração + fallback
via LibreOffice para arquivos malformados).

Uso (de dentro de data/anp/, via toolbox):
    docker-run.sh scripts/anp_extract_batch.py

    # Para reprocessar mesmo os que já têm .parquet gerado:
    docker-run.sh scripts/anp_extract_batch.py --forcar
"""

import argparse
import os
import sys
import time

# Reaproveita as funções já testadas em anp_extract.py (mesma pasta)
from anp_extract import extract_municipios

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DADOS_DIR = os.path.join(SCRIPT_DIR, "..", "dados")
PARQUET_DIR = os.path.join(SCRIPT_DIR, "..", "parquet")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--forcar", action="store_true",
        help="Reprocessa mesmo arquivos que já têm .parquet gerado (default: pula)"
    )
    args = parser.parse_args()

    os.makedirs(PARQUET_DIR, exist_ok=True)

    files = sorted(f for f in os.listdir(DADOS_DIR) if f.lower().endswith(".xlsx"))
    print(f"Encontrados {len(files)} arquivos em {DADOS_DIR}\n")

    sucesso = []
    fallback = []
    pulados = []
    erros = []

    inicio = time.time()

    for i, filename in enumerate(files, 1):
        xlsx_path = os.path.join(DADOS_DIR, filename)
        base_name = os.path.splitext(filename)[0]
        parquet_path = os.path.join(PARQUET_DIR, base_name + ".parquet")

        if os.path.exists(parquet_path) and not args.forcar:
            pulados.append(filename)
            print(f"[{i}/{len(files)}] {filename} ... já existe, pulando")
            continue

        print(f"[{i}/{len(files)}] {filename} ... ", end="", flush=True)
        try:
            df, used_fallback = extract_municipios(xlsx_path)
            df["ARQUIVO_ORIGEM"] = filename
            df.to_parquet(parquet_path, index=False)

            sucesso.append(filename)
            if used_fallback:
                fallback.append(filename)
                print(f"OK ({len(df)} linhas, via fallback LibreOffice)")
            else:
                print(f"OK ({len(df)} linhas)")
        except Exception as e:
            erros.append((filename, str(e)))
            print(f"ERRO: {e}")

    duracao = time.time() - inicio

    print("\n" + "=" * 70)
    print("RESUMO DO LOTE")
    print("=" * 70)
    print(f"Total de arquivos:        {len(files)}")
    print(f"Processados com sucesso:  {len(sucesso)}")
    print(f"  (dos quais via fallback LibreOffice: {len(fallback)})")
    print(f"Pulados (já existiam):    {len(pulados)}")
    print(f"Falharam:                 {len(erros)}")
    print(f"Tempo total:              {duracao:.1f}s")

    if erros:
        print(f"\nArquivos com erro:")
        for filename, err in erros:
            print(f"  - {filename}: {err}")

    if erros:
        sys.exit(1)  # sinaliza falha pro Airflow, no futuro, se chamado como task


if __name__ == "__main__":
    main()
