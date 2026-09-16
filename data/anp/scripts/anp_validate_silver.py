"""
Gate de qualidade do silver — valida os arquivos .parquet das 5 abas
(MUNICIPIOS, ESTADOS, REGIOES, CAPITAIS, BRASIL) antes de liberar a
pipeline para a etapa de agregação (gold).

Cada aba tem seu próprio schema (colunas geográficas diferentes: ESTADO/
MUNICÍPIO, REGIAO/ESTADOS, REGIAO, BRASIL), então as checagens de schema
e chave natural são parametrizadas por aba.

Checagens realizadas por aba (baseadas na exploração KDD que confirmou
o padrão esperado dos dados em MUNICIPIOS, e replicadas para as demais):
    1. Schema: colunas esperadas presentes, com os tipos certos
    2. Nulos: nenhuma linha com valor nulo em nenhuma coluna
    3. Consistência: PREÇO MÍNIMO <= PREÇO MÉDIO <= PREÇO MÁXIMO sempre
    4. Duplicatas: nenhuma linha duplicada pela chave natural da aba

Se qualquer checagem falhar em qualquer aba, o script termina com código
de erro (exit 1), interrompendo a pipeline antes de propagar dado
suspeito para o gold.

Variáveis de ambiente esperadas (mesmas do restante do projeto):
    MINIO_ENDPOINT        (default: "minio:9000")
    MINIO_ROOT_USER
    MINIO_ROOT_PASSWORD
    MINIO_SILVER_BUCKET   (default: "silver")
    MINIO_SILVER_PREFIX_BASE (default: "anp/")

Uso (precisa estar na rede do projeto, para resolver o hostname "minio"):
    docker run --rm -it \\
        --network data-engineering_data-net \\
        --env-file /mnt/work/data-engineering/.env \\
        -v "$(pwd)":/app \\
        py-toolbox scripts/anp_validate_silver.py
"""

import io
import os
import sys

import pandas as pd
from minio import Minio

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")

SILVER_BUCKET = os.environ.get("MINIO_SILVER_BUCKET", "silver")
SILVER_PREFIX_BASE = os.environ.get("MINIO_SILVER_PREFIX_BASE", "anp/")

# Colunas comuns a todas as abas (preço, produto, datas, etc.)
COLUNAS_COMUNS = {
    "DATA INICIAL": "datetime64[ns]",
    "DATA FINAL": "datetime64[ns]",
    "PRODUTO": "object",
    "NÚMERO DE POSTOS PESQUISADOS": "int64",
    "UNIDADE DE MEDIDA": "object",
    "PREÇO MÉDIO REVENDA": "float64",
    "DESVIO PADRÃO REVENDA": "float64",
    "PREÇO MÍNIMO REVENDA": "float64",
    "PREÇO MÁXIMO REVENDA": "float64",
    "COEF DE VARIAÇÃO REVENDA": "float64",
}

# Schema e chave natural específicos de cada aba (colunas geográficas
# variam conforme o nível de agregação do relatório)
ABAS = {
    "municipios": {
        "colunas_extras": {"ESTADO": "object", "MUNICÍPIO": "object"},
        "chave_natural": ["DATA INICIAL", "DATA FINAL", "ESTADO", "MUNICÍPIO", "PRODUTO"],
    },
    "capitais": {
        "colunas_extras": {"ESTADO": "object", "MUNICÍPIO": "object"},
        "chave_natural": ["DATA INICIAL", "DATA FINAL", "ESTADO", "MUNICÍPIO", "PRODUTO"],
    },
    "estados": {
        "colunas_extras": {"REGIAO": "object", "ESTADOS": "object"},
        "chave_natural": ["DATA INICIAL", "DATA FINAL", "REGIAO", "ESTADOS", "PRODUTO"],
    },
    "regioes": {
        "colunas_extras": {"REGIAO": "object"},
        "chave_natural": ["DATA INICIAL", "DATA FINAL", "REGIAO", "PRODUTO"],
    },
    "brasil": {
        "colunas_extras": {"BRASIL": "object"},
        "chave_natural": ["DATA INICIAL", "DATA FINAL", "BRASIL", "PRODUTO"],
    },
}


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


def carregar_aba(client: Minio, aba: str) -> pd.DataFrame:
    """Carrega todos os .parquet de uma aba em um único DataFrame."""
    prefix = SILVER_PREFIX_BASE.rstrip("/") + "/" + aba + "/"
    objetos = [
        obj.object_name
        for obj in client.list_objects(SILVER_BUCKET, prefix=prefix, recursive=True)
        if obj.object_name.lower().endswith(".parquet")
    ]

    if not objetos:
        raise FileNotFoundError(f"Nenhum arquivo .parquet encontrado em {SILVER_BUCKET}/{prefix}")

    dataframes = []
    for object_name in objetos:
        response = client.get_object(SILVER_BUCKET, object_name)
        try:
            buffer = io.BytesIO(response.read())
            dataframes.append(pd.read_parquet(buffer))
        finally:
            response.close()
            response.release_conn()

    return pd.concat(dataframes, ignore_index=True)


def checar_schema(df: pd.DataFrame, colunas_esperadas: dict) -> list[str]:
    problemas = []

    colunas_faltando = set(colunas_esperadas) - set(df.columns)
    if colunas_faltando:
        problemas.append(f"Colunas faltando: {colunas_faltando}")

    for coluna, tipo_esperado in colunas_esperadas.items():
        if coluna not in df.columns:
            continue
        tipo_real = str(df[coluna].dtype)

        # Datetime: aceita qualquer precisão (ns/us/ms) — a precisão exata
        # pode variar conforme a lib/versão que escreveu o parquet (ex.:
        # pyarrow grava "us" para compatibilidade com o Spark, que não lê
        # "ns"), sem que isso represente um problema de qualidade de dado.
        if tipo_esperado.startswith("datetime64") and tipo_real.startswith("datetime64"):
            continue

        if tipo_esperado not in tipo_real and tipo_real not in tipo_esperado:
            problemas.append(
                f"Coluna '{coluna}': esperado tipo compatível com '{tipo_esperado}', encontrado '{tipo_real}'"
            )

    return problemas


def checar_nulos(df: pd.DataFrame) -> list[str]:
    problemas = []
    nulos = df.isnull().sum()
    for coluna, qtd in nulos[nulos > 0].items():
        problemas.append(f"Coluna '{coluna}': {qtd} valores nulos")
    return problemas


def checar_consistencia_precos(df: pd.DataFrame) -> list[str]:
    problemas = []
    inconsistentes = df[
        (df["PREÇO MÍNIMO REVENDA"] > df["PREÇO MÉDIO REVENDA"])
        | (df["PREÇO MÉDIO REVENDA"] > df["PREÇO MÁXIMO REVENDA"])
    ]
    if len(inconsistentes) > 0:
        problemas.append(f"{len(inconsistentes)} linhas com PREÇO MÍNIMO > MÉDIO ou MÉDIO > MÁXIMO")
    return problemas


def checar_duplicatas(df: pd.DataFrame, chave_natural: list) -> list[str]:
    problemas = []
    qtd_duplicatas = df.duplicated(subset=chave_natural).sum()
    if qtd_duplicatas > 0:
        problemas.append(f"{qtd_duplicatas} linhas duplicadas pela chave natural {chave_natural}")
    return problemas


def validar_aba(client: Minio, aba: str, config: dict) -> tuple[bool, list[str]]:
    print(f"\n{'='*70}\nValidando aba: {aba.upper()}\n{'='*70}")

    try:
        df = carregar_aba(client, aba)
    except FileNotFoundError as e:
        print(f"[FALHOU] Carregamento: {e}")
        return False, [str(e)]

    print(f"Total de linhas carregadas: {len(df)}")

    colunas_esperadas = {**COLUNAS_COMUNS, **config["colunas_extras"]}

    checagens = {
        "Schema": checar_schema(df, colunas_esperadas),
        "Nulos": checar_nulos(df),
        "Consistência de preços": checar_consistencia_precos(df),
        "Duplicatas": checar_duplicatas(df, config["chave_natural"]),
    }

    todos_problemas = []
    for nome_checagem, problemas in checagens.items():
        if problemas:
            print(f"[FALHOU] {nome_checagem}:")
            for p in problemas:
                print(f"  - {p}")
            todos_problemas.extend(problemas)
        else:
            print(f"[OK] {nome_checagem}")

    return len(todos_problemas) == 0, todos_problemas


def main():
    client = get_client()

    resultado_geral = {}

    for aba, config in ABAS.items():
        aprovado, problemas = validar_aba(client, aba, config)
        resultado_geral[aba] = aprovado

    print("\n" + "=" * 70)
    print("RESUMO DO GATE DE QUALIDADE")
    print("=" * 70)

    for aba, aprovado in resultado_geral.items():
        status = "APROVADO" if aprovado else "REPROVADO"
        print(f"  {aba.upper():<15} {status}")

    if all(resultado_geral.values()):
        print("\nGATE LIBERADO: todas as abas aprovadas. Pode prosseguir para o gold.")
    else:
        print("\nGATE BLOQUEADO: uma ou mais abas reprovadas. "
              "Pipeline interrompida antes da etapa de gold.")
        sys.exit(1)


if __name__ == "__main__":
    main()
