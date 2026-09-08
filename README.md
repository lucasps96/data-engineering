# Data Engineering — Lakehouse Local com Docker

Projeto de arquitetura de dados desenvolvido como parte da minha pós-graduação em Data Science (UTFPR), baseado no material e no passo a passo do professor da disciplina. Este repositório é um fork adaptado para rodar em um ambiente próprio (servidor doméstico), com ajustes de configuração, segurança e documentação dos problemas reais enfrentados durante a implantação.

> Baseado no projeto original do professor da disciplina. As modificações aqui presentes incluem ajustes de portas, permissões, segurança de credenciais e documentação de troubleshooting para rodar em um ambiente com outros serviços já em uso.

## Motivação

A escolha de rodar este projeto em um servidor doméstico (homelab), em vez de apenas seguir o passo a passo em um ambiente isolado, foi intencional: a ideia é usar esse ambiente controlado para praticar problemas reais de infraestrutura — conflito de portas, permissões de arquivo entre host e container, gerenciamento seguro de credenciais — antes de aplicar esse mesmo tipo de arquitetura em um ambiente de produção real (ex: servidor de uma empresa).

Rodar em um servidor que já hospeda outros serviços simula justamente o tipo de cenário que um tutorial em ambiente limpo não expõe: recursos concorrentes, portas já ocupadas, e a necessidade de isolar e documentar cada decisão de configuração. A seção de Troubleshooting abaixo é o resultado direto dessa prática.

**Status atual:** acompanhando o passo a passo da disciplina — atualmente no **passo 6**, mantendo o fork sincronizado com os ajustes necessários para meu ambiente.

## Arquitetura

A stack simula um ambiente de **lakehouse** local, orquestrando as seguintes camadas:

| Componente | Função |
|---|---|
| **Airflow** | Orquestração de pipelines (DAGs de ingestão e transformação) |
| **Spark** (master + workers + thrift server) | Processamento distribuído dos dados |
| **Minio** | Object storage compatível com S3 (camadas bronze/silver/gold do data lake) |
| **dbt** | Transformações e modelagem analítica sobre os dados |
| **Superset** | Visualização e exploração dos dados processados |
| **Postgres** | Metastore do Airflow |

*(Inserir aqui um diagrama simples do fluxo de dados: ingestão → Minio → Spark/dbt → Superset)*

## Como rodar

**1. Clonar o repositório:**
```bash
git clone https://github.com/lucasps96/data-engineering.git
cd data-engineering
```

**2. Criar o arquivo `.env`** na raiz do projeto (não incluído no repositório por segurança — veja a seção de Configuração de credenciais abaixo).

**3. Subir a stack:**
```bash
docker-compose up -d
```

Serviços disponíveis após subir:

| Serviço | URL local |
|---|---|
| Airflow | http://localhost:8082 |
| Spark Master UI | http://localhost:8081 |
| Superset | http://localhost:8088 |
| Minio Console | http://localhost:9001 |
| DBT Docs | http://localhost:8091 |

> As portas podem variar em relação ao projeto original — veja a seção de Troubleshooting para entender por quê.

## Configuração de credenciais (.env)

Todas as credenciais e chaves sensíveis (secret keys, senhas de banco, usuários admin) foram movidas para variáveis de ambiente, fora do controle de versão. Crie um arquivo `.env` na raiz do projeto com o seguinte modelo:

```
# Airflow - webserver
AIRFLOW_SECRET_KEY=<gerar com: python3 -c "import secrets; print(secrets.token_hex(16))">
AIRFLOW_FERNET_KEY=<gerar com: python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">

# Airflow - banco de metadados
AIRFLOW_DB_USER=<seu usuário>
AIRFLOW_DB_PASSWORD=<sua senha>
AIRFLOW_DB_NAME=<nome do banco>

# Airflow - usuário admin da UI
AIRFLOW_ADMIN_USER=<usuário admin>
AIRFLOW_ADMIN_PASSWORD=<senha admin>
AIRFLOW_ADMIN_EMAIL=<seu email>

# Minio
MINIO_ROOT_USER=<usuário root do Minio>
MINIO_ROOT_PASSWORD=<senha root do Minio>

# Superset
SUPERSET_SECRET_KEY=<gerar uma chave própria>
SUPERSET_ADMIN_USER=<usuário admin>
SUPERSET_ADMIN_PASSWORD=<senha admin>
SUPERSET_ADMIN_EMAIL=<seu email>
```

O `.env` está no `.gitignore` e nunca deve ser commitado. Para validar se as variáveis foram carregadas corretamente antes de subir os containers:
```bash
docker-compose config
```

## Troubleshooting

Ao rodar este projeto em um servidor que já hospeda outros serviços (torrent client, media server, etc.), encontrei os seguintes problemas — documentados aqui para referência futura e para quem for recriar o setup em ambientes não "limpos".

### 1. Conflito de porta 8080 (serviço já rodando no host)

**Sintoma:** `Error starting userland proxy: listen tcp4 0.0.0.0:8080: bind: address already in use`

**Causa:** outro serviço no host (no meu caso, um cliente de torrent — qbittorrent-nox) já ocupava a porta 8080.

**Diagnóstico:**
```bash
sudo ss -tlnp | grep 8080
```

**Solução:** remapear a porta do Airflow no `docker-compose.yml` (host:container), mantendo a porta interna do container:
```yaml
ports:
  - "8082:8080"
```

### 2. Porta já alocada por container anterior

**Sintoma:** `Bind for 0.0.0.0:8081 failed: port is already allocated`

**Causa:** containers antigos do Airflow ficaram parados em estado `Created` (nunca chegaram a rodar) ainda segurando a porta, além de colisão com a porta padrão da UI do Spark Master (também usa 8080/8081 internamente).

**Diagnóstico:**
```bash
docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

**Solução:** remover containers travados e escolher portas de host distintas para cada serviço:
```bash
docker rm -f <container_travado>
```

### 3. Permissão negada nos logs do Airflow

**Sintoma:**
```
PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/scheduler'
```
O container ficava em loop de retry (`ERROR! Maximum number of retries (20) reached`) e nunca chegava a subir o webserver, mesmo aparecendo como `Up` no `docker ps` — um sintoma enganoso, já que por fora parecia estar funcionando.

**Causa:** diretórios montados do host (`airflow/`, `dbt_lakehouse/`) com dono/permissões incompatíveis com o usuário usado dentro do container (UID 50000, padrão da imagem oficial do Airflow).

**Solução:**
```bash
sudo chown -R 50000:0 airflow/
sudo chown -R 50000:0 dbt_lakehouse/
docker-compose up -d --force-recreate airflow
```

### 4. Erro de CSRF ao logar no Airflow ("The CSRF session token is missing")

**Causa:** ausência de uma `AIRFLOW__WEBSERVER__SECRET_KEY` fixa. Sem essa variável, o Airflow gera uma chave aleatória a cada boot do container, invalidando sessões/cookies sempre que o container é recriado — o que acontecia com frequência durante os ajustes de porta e permissão.

**Solução:** definir uma chave fixa (ver seção de Configuração de credenciais):
```bash
python3 -c "import secrets; print(secrets.token_hex(16))"
```
Após configurar, recriar o container e limpar cookies do navegador antes de logar novamente.

### 5. Credenciais expostas no docker-compose.yml

**Problema:** o compose original trazia secrets (chave do webserver, fernet key, senhas de banco, credenciais do Minio e do Superset) diretamente hardcoded no YAML — o que fica exposto no histórico do Git em um repositório público.

**Solução:** todas as credenciais foram migradas para variáveis de ambiente via `.env` (fora do controle de versão), referenciadas no compose com a sintaxe `${VARIAVEL}`. Detalhes na seção de Configuração de credenciais acima.

## Créditos

Projeto original e passo a passo: professor da disciplina de Arquitetura de Dados (pós-graduação em Data Science, UTFPR). Adaptações, troubleshooting e documentação: Lucas.
