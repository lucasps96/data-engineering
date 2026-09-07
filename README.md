# Engenharia de Dados

Projeto prático de um **lakehouse** completo rodando na sua máquina: MinIO (armazenamento),
Spark + Delta Lake (processamento), Hive metastore (catálogo), Airflow (orquestração),
DBT (transformação) e Superset (visualização).

Este README é o **guia de execução**: siga de cima para baixo e você terá o lakehouse
funcionando na sua máquina.

> **Prefere não instalar nada na sua máquina?** Dá para rodar tudo no navegador, pelo
> GitHub Codespaces: faça o fork (seção 2) e siga a **seção 4** no lugar da 3.

---

## 1. Pré-requisitos

Instale na sua máquina:

* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* [Docker Desktop](https://www.docker.com/products/docker-desktop/)
* [VSCode](https://code.visualstudio.com/)

A instalação depende do seu sistema operacional. Preparei guias para
[Windows](./_support/windows.md), [macOS](./_support/macos.md) e [Ubuntu](./_support/ubuntu.md).

Você também precisa de uma conta no [GitHub](https://github.com/).

> **Está no Linux?** Pode usar o Docker Engine em vez do Docker Desktop — é mais leve e
> funciona igual. Só garanta que instalou o plugin do Compose v2 (`docker compose`, com
> espaço, não `docker-compose` com hífen).

---

## 2. Clonar o projeto

Acesse https://github.com/weslleymoura/data-engineering e crie um **fork**. Isso cria uma
cópia do projeto na sua conta do GitHub.

<img src="_support/git-fork.png" width="400">

Abra o terminal, vá até a pasta onde quer salvar o projeto e clone **o seu fork**:

```
git clone <<url-do-seu-repositorio>>
```

A URL está na página do seu fork, no botão verde de código (use a opção HTTPS):

<img src="_support/git-clone.png" width="400">

---

## 3. Subir os serviços

Entre na pasta do projeto (`data-engineering`) e libere as permissões das pastas que os
containers precisam escrever:

```
# dar permissão para a UID exata do airflow
sudo chown -R 50000:0 airflow/ 
sudo chown -R 50000:0 dbt_lakehouse/
```

Suba tudo:

```
docker compose up -d --build
```

> A primeira execução baixa vários GB de imagens e constrói três delas — leve de 15 a 30
> minutos, dependendo da sua internet. Dê pelo menos **8 GB de RAM** ao Docker Desktop
> (Settings → Resources), senão o Spark e o Superset morrem ao iniciar.

Acompanhe o progresso com:

```
docker compose ps
docker compose logs -f airflow
```

### Conferindo se deu certo

O ambiente está correto se você conseguir abrir estes endereços:

| Serviço | URL | Usuário | Senha |
| --- | --- | --- | --- |
| MinIO (armazenamento) | http://localhost:9001 | `minio` | `minio123` |
| Airflow (orquestração) | http://localhost:8080 | `admin` | `admin` |
| Spark Master | http://localhost:8081 | – | – |
| Superset (visualização) | http://localhost:8088 | `admin` | `admin` |

> A porta **9001** é o painel do MinIO. A **9000** é a API — se abrir essa no navegador,
> você vê um XML ou um erro, não a tela de login.
>
> No **Codespaces**, os endereços são outros: abra pela aba **PORTS**. Os usuários e senhas
> são os mesmos.
>
> Estas senhas são descartáveis e valem só para o ambiente local do curso.

---

## 4. Alternativa: rodar no GitHub Codespaces

Em vez de instalar Docker na sua máquina, você pode rodar tudo no navegador. O projeto é
o mesmo — os comandos das próximas seções funcionam igual lá dentro.

Antes de começar: você precisa ter feito o **fork** (seção 2). O Codespaces tem que ser
criado a partir do **seu** fork, senão você não consegue salvar seu trabalho.

### Passo 1 — Preparar o ambiente (opcional)

No **seu fork**: **Settings** → **Codespaces** → **Set up prebuild** → branch `main` →
**Create**.

O prebuild deixa parte do ambiente pronta com antecedência, então o Codespaces abre mais
rápido. É opcional: sem ele tudo funciona igual, só leva alguns minutos a mais na criação.
Ele também consome um pouco da sua cota, então pule se estiver economizando.

### Passo 2 — Criar o Codespaces

No seu fork: botão verde **Code** → aba **Codespaces** → **Create codespace on main**.

O ambiente leva alguns minutos para montar. Quando o editor abrir, você está pronto.

### Passo 3 — Subir os serviços

No terminal do Codespaces, rode os três comandos **na ordem**:

```
docker compose build
docker builder prune -af
docker compose up -d
```

De 15 a 30 minutos na primeira vez — ele constrói as imagens ali dentro.

> O comando do meio não é opcional. O disco do Codespaces é de 32 GB, e o cache
> gerado durante o build ocupa quase 9 GB que não servem para mais nada depois que
> as imagens ficam prontas. Sem apagá-lo, o disco enche no meio do pipeline.

Confira se subiu:

```
docker compose ps
```

Os 9 containers devem aparecer. Você **não** precisa rodar o `chmod` da seção 3: ele já
roda sozinho na criação do ambiente.

### Passo 4 — Abrir os serviços

Use a aba **PORTS**, na barra inferior ao lado do TERMINAL. Passe o mouse na porta que
quer abrir e clique no ícone de globo.

| Porta | Serviço |
| --- | --- |
| 8080 | Airflow |
| 8081 | Spark Master |
| 8088 | Superset |
| 9001 | MinIO |
| 8091 | DBT Docs |

Usuários e senhas são os mesmos da tabela da seção 3. **Não use `localhost`** no
navegador: no Codespaces cada porta tem uma URL própria, gerada pelo GitHub.

### Passo 5 — Seguir o guia

Agora siga a **seção 5** normalmente. Todos os comandos funcionam igual.

### Passo 6 — Encerrar quando terminar de estudar

Isso importa: o Codespaces é gratuito **até um limite mensal**, e o limite se esgota mesmo
quando você não está usando.

São dois consumos separados:

| O que conta | Cota gratuita | Quanto dura na nossa máquina |
| --- | --- | --- |
| Tempo ligado | 120 core-hours/mês | ~15 horas de uso |
| Ambiente existindo | 15 GB-month | ~7 dias, mesmo parado |

Repare na segunda linha: **o armazenamento conta enquanto o ambiente existir**, ligado ou
não. Parar não é suficiente.

**Ao terminar uma sessão de estudo**, pare o ambiente — assim ele para de consumir tempo,
mas o seu trabalho continua lá para a próxima vez:

* Menu **☰** (canto superior esquerdo) → **Stop Current Codespace**

**Ao terminar de vez** (fim de um módulo, ou se for ficar dias sem usar), apague — é a
única forma de parar o consumo de armazenamento:

1. Faça `git push` de qualquer trabalho que queira guardar, porque apagar remove tudo
2. Acesse [github.com/codespaces](https://github.com/codespaces)
3. No menu **`...`** do ambiente → **Delete**

Criar de novo depois é rápido, e você não perde nada que já tenha enviado para o seu fork.

> Se a cota acabar, o Codespaces simplesmente para de abrir até o mês virar — não gera
> cobrança nenhuma. Mas você fica sem ambiente, então vale apagar quando não estiver
> usando.

---

## 5. Montando o lakehouse — pipeline de exemplo

Neste momento os serviços estão no ar, mas **o lakehouse está vazio**. Os passos abaixo são
os que constroem os dados, e a **ordem importa**: cada um depende do anterior.

### Passo 1 — Rodar o pipeline de ingestão

Abra o [Airflow](http://localhost:8080), encontre a DAG `lakehouse_pipeline`, **despause**
no botão da esquerda e clique no ▶ para executar.

As duas tasks devem ficar verdes:

* `bronze_to_silver` lê o CSV da camada bronze e grava como Delta na silver
* `silver_to_gold` agrega por cliente e grava o resultado na gold

Você pode acompanhar no [Spark Master](http://localhost:8081) e ver os arquivos aparecerem
no [MinIO](http://localhost:9001).

### Passo 2 — Registrar a tabela no catálogo

```
docker exec -it spark-master beeline -u jdbc:hive2://spark-thrift-server:10000 -e "CREATE TABLE IF NOT EXISTS default.order_summary USING DELTA LOCATION 's3a://gold/warehouse/default/order_summary';"
```

Deve responder `No rows selected`.

**Por que este passo existe?** O pipeline grava usando a própria sessão Spark, que tem um
catálogo separado. O Superset e o DBT conversam com outro catálogo — o do Thrift server. Este
comando registra a tabela lá.

> Você só faz isso **uma vez**. O catálogo fica guardado num volume e sobrevive a
> `docker compose down`, restart e reinício da máquina. Só é apagado com
> `docker compose down --volumes`, que apaga os dados junto.

> ⚠️ A ordem importa. Se rodar este passo **antes** do passo 1, você registra uma tabela
> apontando para um lugar vazio: o comando passa sem erro e só quebra depois, na primeira
> consulta.

### Passo 3 — Rodar o projeto DBT

De volta ao Airflow, despause e execute a DAG `dbt_run_lakehouse_project`.

### Passo 4 — Conferir

```
docker exec -it spark-master beeline -u jdbc:hive2://spark-thrift-server:10000 -e "SELECT SUM(total_amount) FROM marts.fct_summary;"
```

Se voltar um número, a corrente inteira funciona: MinIO → Spark → Delta → catálogo → DBT.

---

## 6. Mantendo seu fork atualizado

Seu fork é uma **fotografia** tirada no momento em que você clicou em Fork. Ele não
acompanha o repositório original sozinho, e o seu `git pull` busca do **seu** fork — então
correções publicadas aqui não chegam até você automaticamente.

Faça isso sempre que algo não funcionar como descrito no guia, e uma vez antes de começar
cada nova parte do curso.

O jeito fácil é o botão **Sync fork**, na página do seu fork no GitHub. Ele aparece logo
acima da lista de arquivos sempre que sua cópia está atrasada.

Depois, na sua máquina:

```
git pull
docker compose up -d --build
```

<details>
<summary>Alternativa pela linha de comando (e o que fazer em caso de conflito)</summary>

A primeira linha você roda só uma vez, para sempre:

```
git remote add upstream https://github.com/weslleymoura/data-engineering.git
git fetch upstream
git merge upstream/main
git push origin main
```

Se o `git merge` acusar conflito, é porque você alterou as mesmas linhas que foram
corrigidas. Para ficar com a sua versão: `git checkout --ours <arquivo>`. Para ficar com a
correção: `git checkout --theirs <arquivo>`. Depois `git add` no arquivo e `git commit`.

Algumas correções mudam **como** os arquivos são baixados, e não o conteúdo deles. Nesses
casos é preciso recarregar os arquivos. Commite ou guarde seu trabalho antes, porque o
`reset --hard` descarta alterações não commitadas:

```
git rm --cached -r .
git reset --hard
```

</details>

---

## 7. Comandos úteis

**Conectar o Superset ao lakehouse** — use esta string de conexão:

```
hive://spark-thrift-server:10000/default
```

Depois de conectado, você pode consultar as tabelas do DBT:

```
SELECT SUM(total_amount) AS total_amount FROM marts.fct_summary
```

**Ver a documentação e a linhagem do DBT** — as DAGs de DBT já geram a documentação. Para
servir na porta 8091:

```
docker exec -d airflow bash -c "cd /home/airflow/dbt_lakehouse/target && exec python3 -m http.server 8091"
```

Acesse http://localhost:8091 e clique no ícone azul no canto inferior direito para ver o
grafo de linhagem.

**Explorar o catálogo:**

```
docker exec -it spark-master beeline -u jdbc:hive2://spark-thrift-server:10000 -e "SHOW SCHEMAS;"
docker exec -it spark-master beeline -u jdbc:hive2://spark-thrift-server:10000 -e "SHOW TABLES IN marts;"
```

**Explorar os arquivos no MinIO:**

```
docker exec -it mc mc ls -r local/gold/warehouse/
```

**Reconstruir um modelo específico do DBT** (e tudo que depende dele):

```
docker exec -it airflow bash -c "cd /home/airflow/dbt_lakehouse && dbt build --select fct_summary"
```

**Desligar os serviços:**

```
docker compose down
```

Isso **preserva** tudo — o lakehouse volta exatamente como você deixou, catálogo incluído.

Para zerar de verdade e recomeçar do passo 1:

```
docker compose down --volumes --remove-orphans
```
