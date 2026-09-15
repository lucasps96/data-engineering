"""
Extração da aba MUNICIPIOS de um arquivo "resumo semanal" da ANP (bronze)
para um Parquet limpo (silver intermediário).

Este script cuida apenas da extração/reparo do Excel — não faz
transformação de negócio (tipagem fina, normalização de texto, etc.),
que fica a cargo da etapa seguinte, em Spark.

Uso (de dentro de data/anp/, via toolbox):
    docker-run.sh scripts/anp_extract.py --arquivo dados/resumo_semanal_lpc_2024-12-01_2024-12-07.xlsx

    # Saída padrão: parquet/<mesmo nome>.parquet
    # Para especificar destino:
    docker-run.sh scripts/anp_extract.py --arquivo dados/arquivo.xlsx --saida parquet/saida.parquet

Requisitos:
    pip install openpyxl pandas pyarrow --break-system-packages
    (libreoffice precisa estar disponível no container, para o fallback)
"""

import argparse
import os
import subprocess
import sys
import tempfile
import warnings

import openpyxl
import pandas as pd

warnings.filterwarnings("ignore")

SHEET_NAME = "MUNICIPIOS"
HEADER_ROW_INDEX = 9  # linha 10 (0-indexed) — confirmado na inspeção manual

EXPECTED_COLUMNS = [
    "DATA INICIAL",
    "DATA FINAL",
    "ESTADO",
    "MUNICÍPIO",
    "PRODUTO",
    "NÚMERO DE POSTOS PESQUISADOS",
    "UNIDADE DE MEDIDA",
    "PREÇO MÉDIO REVENDA",
    "DESVIO PADRÃO REVENDA",
    "PREÇO MÍNIMO REVENDA",
    "PREÇO MÁXIMO REVENDA",
    "COEF DE VARIAÇÃO REVENDA",
]
N_COLS = len(EXPECTED_COLUMNS)


def try_open_workbook(path: str):
    """
    Tenta abrir o workbook diretamente. Se falhar (arquivo com formato
    'strict' OOXML malformado — visto em ~9 dos 194 arquivos, concentrados
    em dez/2022-abr/2023), converte via LibreOffice headless e tenta de novo.

    Retorna (workbook, usou_fallback: bool).
    """
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        if wb.sheetnames:
            return wb, False
    except Exception:
        pass

    with tempfile.TemporaryDirectory() as tmpdir:
        result = subprocess.run(
            [
                "libreoffice", "--headless", "--calc", "--convert-to", "xlsx",
                "--outdir", tmpdir, path,
            ],
            capture_output=True,
            text=True,
            timeout=90,
        )
        converted_path = os.path.join(tmpdir, os.path.basename(path))
        if not os.path.exists(converted_path):
            raise RuntimeError(
                f"LibreOffice não conseguiu converter o arquivo. "
                f"stderr: {result.stderr[:300]}"
            )
        # Copia pra FORA do tmpdir (que será apagado ao sair deste bloco)
        # usando o diretório temp padrão do sistema, para podermos reabrir
        # com openpyxl depois que o tmpdir já não existe mais.
        fd, persistent_path = tempfile.mkstemp(suffix=".xlsx")
        os.close(fd)
        with open(converted_path, "rb") as src, open(persistent_path, "wb") as dst:
            dst.write(src.read())

    try:
        wb = openpyxl.load_workbook(persistent_path, read_only=True, data_only=True)
    finally:
        os.remove(persistent_path)
    return wb, True


def extract_municipios(path: str) -> pd.DataFrame:
    """
    Extrai a aba MUNICIPIOS de um arquivo bronze e retorna um DataFrame
    já com o cabeçalho correto e apenas as colunas esperadas (descarta
    colunas extras vazias, vistas em pelo menos 1 dos 194 arquivos).
    """
    wb, used_fallback = try_open_workbook(path)
    try:
        if SHEET_NAME not in wb.sheetnames:
            raise ValueError(
                f"Aba '{SHEET_NAME}' não encontrada. Abas disponíveis: {wb.sheetnames}"
            )
        ws = wb[SHEET_NAME]

        header_row = next(
            ws.iter_rows(min_row=HEADER_ROW_INDEX + 1, max_row=HEADER_ROW_INDEX + 1, values_only=True)
        )
        header = [str(h).strip() if h is not None else None for h in header_row[:N_COLS]]

        if header != EXPECTED_COLUMNS:
            raise ValueError(
                f"Cabeçalho inesperado em {os.path.basename(path)}.\n"
                f"  Esperado: {EXPECTED_COLUMNS}\n"
                f"  Encontrado: {header}"
            )

        data_rows = []
        for row in ws.iter_rows(min_row=HEADER_ROW_INDEX + 2, values_only=True):
            # Ignora linhas totalmente vazias (comuns no final da planilha)
            if all(c is None for c in row):
                continue
            data_rows.append(row[:N_COLS])

        df = pd.DataFrame(data_rows, columns=EXPECTED_COLUMNS)
        return df, used_fallback
    finally:
        wb.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arquivo", required=True, help="Caminho do .xlsx de entrada (bronze)")
    parser.add_argument("--saida", default=None, help="Caminho do .parquet de saída (default: parquet/<nome>.parquet)")
    args = parser.parse_args()

    if not os.path.exists(args.arquivo):
        print(f"ERRO: arquivo não encontrado: {args.arquivo}", file=sys.stderr)
        sys.exit(1)

    if args.saida is None:
        base_name = os.path.splitext(os.path.basename(args.arquivo))[0]
        script_dir = os.path.dirname(os.path.abspath(__file__))
        out_dir = os.path.join(script_dir, "..", "parquet")
        os.makedirs(out_dir, exist_ok=True)
        saida = os.path.join(out_dir, base_name + ".parquet")
    else:
        saida = args.saida
        os.makedirs(os.path.dirname(os.path.abspath(saida)) or ".", exist_ok=True)

    print(f"Lendo: {args.arquivo}")
    df, used_fallback = extract_municipios(args.arquivo)

    if used_fallback:
        print("  (precisou de fallback via LibreOffice)")

    # Rastreabilidade: guarda de qual arquivo bronze cada linha veio
    df["ARQUIVO_ORIGEM"] = os.path.basename(args.arquivo)

    df.to_parquet(saida, index=False)

    print(f"Linhas extraídas: {len(df)}")
    print(f"Colunas: {list(df.columns)}")
    print(f"Salvo em: {saida}")

    # Mostra uma amostra pra conferência visual rápida
    print("\nAmostra (5 primeiras linhas):")
    print(df.head().to_string())


if __name__ == "__main__":
    main()
