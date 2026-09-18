"""
Scraper para o Levantamento de Preços de Combustíveis (ANP).

Extrai da página de "últimas semanas pesquisadas" todos os links de
"Preços médios semanais" (.xlsx) e baixa os arquivos ainda não presentes
localmente.

Pasta de destino: <raiz do projeto>/data/anp/dados/ (calculada em
DEST_DIR, relativa a este script — funciona tanto rodando direto no
host quanto dentro do container py-toolbox).

Uso:
    python scrape_anp_precos.py

Requisitos:
    pip install requests beautifulsoup4 --break-system-packages
"""

import os
import re
import time
import logging
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# --- Configuração ---------------------------------------------------------

PAGE_URL = (
    "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/"
    "precos/levantamento-de-precos-de-combustiveis-ultimas-semanas-pesquisadas"
)

import os as _os

# Caminho relativo ao script: scripts/ -> ../dados/
# Funciona tanto rodando direto no host quanto dentro do container
# py-toolbox (onde a pasta "scripts" é montada em /app e "dados" fica
# em /app/../dados, ou seja, um nível acima da montagem).
DEST_DIR = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "dados")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def fetch_page(url: str) -> str:
    """Baixa o HTML da página de listagem."""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def extract_resumo_links(html: str, base_url: str) -> list[dict]:
    """
    Percorre a página e extrai apenas os links de "Preços médios semanais"
    (arquivo "resumo_semanal_lpc*"), ignorando os links "por posto
    revendedor" (que não usamos nesta fase).

    Retorna uma lista de dicts: {"url": ..., "filename": ...}
    """
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]

        # Filtra apenas arquivos xlsx de "resumo_semanal" (preços médios).
        # Ignora "revendas_lpc*" (dados por posto), que ficam fora do escopo.
        if "resumo_semanal_lpc" in href.lower() and href.lower().endswith(".xlsx"):
            full_url = urljoin(base_url, href)
            filename = os.path.basename(full_url.split("?")[0])
            links.append({"url": full_url, "filename": filename})

    # Remove duplicados mantendo a ordem
    seen = set()
    unique_links = []
    for item in links:
        if item["url"] not in seen:
            seen.add(item["url"])
            unique_links.append(item)

    return unique_links


def download_file(url: str, dest_path: str) -> bool:
    """Baixa um arquivo para dest_path. Retorna True se baixou, False se já existia."""
    if os.path.exists(dest_path):
        log.info("Já existe, pulando: %s", os.path.basename(dest_path))
        return False

    log.info("Baixando: %s", os.path.basename(dest_path))
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    with open(dest_path, "wb") as f:
        f.write(resp.content)

    return True


def main():
    os.makedirs(DEST_DIR, exist_ok=True)

    log.info("Buscando lista de arquivos em: %s", PAGE_URL)
    html = fetch_page(PAGE_URL)

    links = extract_resumo_links(html, PAGE_URL)
    log.info("Encontrados %d arquivos de 'resumo semanal' na página.", len(links))

    if not links:
        log.warning(
            "Nenhum link encontrado. A página pode ter mudado de estrutura, "
            "ou o conteúdo é carregado via JavaScript (nesse caso, este "
            "scraper simples não vai funcionar e será necessário usar "
            "Selenium/Playwright)."
        )
        return

    baixados = 0
    ja_existentes = 0

    for item in links:
        dest_path = os.path.join(DEST_DIR, item["filename"])
        try:
            if download_file(item["url"], dest_path):
                baixados += 1
                time.sleep(1)  # pequena pausa entre downloads, por educação com o servidor
            else:
                ja_existentes += 1
        except requests.RequestException as e:
            log.error("Falha ao baixar %s: %s", item["url"], e)

    log.info(
        "Concluído. Novos arquivos baixados: %d | Já existentes: %d | Total na página: %d",
        baixados,
        ja_existentes,
        len(links),
    )


if __name__ == "__main__":
    main()
