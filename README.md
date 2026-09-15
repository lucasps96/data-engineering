# Data Engineering — Lakehouse com Dados de Preços de Combustíveis (ANP)

## Introdução

Este projeto nasce do aprofundamento do estudo sobre o material do professor **Weslley Moura**, na disciplina de Arquitetura de Dados da Especialização em Ciência de Dados da UTFPR. A partir de um fork do repositório original da disciplina, o projeto evoluiu para explorar, na prática, os conceitos de arquitetura de *lakehouse* aplicados sobre uma base de dados real.

## Estrutura de Lakehouse

O projeto segue a arquitetura de *lakehouse* em três camadas, armazenadas no Minio (object storage compatível com S3):

- **Bronze** — dado bruto, cópia fiel da fonte original, sem qualquer tratamento.
- **Silver** — dado limpo, tipado e normalizado, pronto para ser consumido de forma confiável.
- **Gold** — dado agregado e modelado para consumo analítico (dashboards, relatórios, modelos de ML).

## Dataset

A fonte de dados utilizada é o **Levantamento de Preços de Combustíveis**, publicado semanalmente pela ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis).

- **Período coberto:** semanal, desde setembro de 2022 até o presente.
- **Volume:** 194 planilhas (uma por semana pesquisada).
- **Estrutura de cada planilha:** 5 abas — `CAPITAIS`, `MUNICIPIOS`, `ESTADOS`, `REGIOES` e `BRASIL` — cada uma com o mesmo conjunto de colunas, variando apenas o nível de agregação geográfica.
- **Aba utilizada neste projeto:** `MUNICIPIOS`, com granularidade de município × produto × semana.
- **Colunas:**
  - `DATA INICIAL`
  - `DATA FINAL`
  - `ESTADO`
  - `MUNICÍPIO`
  - `PRODUTO`
  - `NÚMERO DE POSTOS PESQUISADOS`
  - `UNIDADE DE MEDIDA`
  - `PREÇO MÉDIO REVENDA`
  - `DESVIO PADRÃO REVENDA`
  - `PREÇO MÍNIMO REVENDA`
  - `PREÇO MÁXIMO REVENDA`
  - `COEF DE VARIAÇÃO REVENDA`

## Fluxo de Orquestração

```
ANP (fonte)
    │
    ▼
Scraping ──► Bronze (Minio)
                │
                ▼
            Extração/Reparo
                │
                ▼
            Silver (Minio)
                │
                ▼
            Gold (Minio)
                │
        ┌───────┴───────┐
        ▼               ▼
    Superset          ML
 (dashboards)     (previsão)

Tudo orquestrado pelo Airflow
```

## Adaptações e Troubleshooting

### Infraestrutura (adaptações para rodar em ambiente self-hosted)

| Erro | Solução |
|---|---|
| Porta 8080 já em uso (conflito com serviço do host ou container travado) | Identificar o processo/container ocupando a porta (`ss -tlnp` / `docker ps -a`) e remapear a porta do serviço afetado no `docker-compose.yml` |
| Permissão negada nos logs do Airflow (`PermissionError` no diretório de logs) | Conceder permissão via ACL para o UID do Airflow, sem alterar o dono original dos arquivos: `setfacl -R -m u:50000:rwx airflow/ dbt_lakehouse/` e `setfacl -R -d -m u:50000:rwx airflow/ dbt_lakehouse/` (para que novos arquivos herdem a mesma permissão) |
| Erro de CSRF ao logar no Airflow | Definir `AIRFLOW__WEBSERVER__SECRET_KEY` fixa via `.env` |
| Credenciais expostas no `docker-compose.yml` | Migrar todas as chaves e senhas para variáveis de ambiente (`.env`) |

### Pipeline de dados (problemas específicos do dataset real)

| Erro | Solução |
|---|---|
| `openpyxl` falha ao abrir arquivos `.xlsx` da ANP (formato OOXML "strict" malformado) | Fallback automático via LibreOffice headless antes de reabrir o arquivo |
| `DockerOperator` sem permissão no socket do Docker | Adicionar o GID do grupo `docker` do host ao serviço `airflow` via `group_add` |
