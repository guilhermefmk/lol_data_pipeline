# League of legends chalenger data match pipeline


Este projeto realiza uma pipeline completa dos dados das partidas do ranque desafiante do jogo eletrônico league of legends fornecidos pela api oficial do jogo e por fim uma análise destes dados, essa análise busca responder perguntas como:

* Quais personagens tem maior e menor taxa de vitória?  
* Quais personagens aplicam a maior quantidade de dano por partida?
* Quais personagens aplicam as maiores quantidades de dano fisico/magico por partida? 
* Quais personagens aplicam mais frequentemente o first blood?

## Arquitetura do projeto
![project architecture](/assets/img/MarineGEO_logo.png "project architecture")
## Setup
### Stack

* Iac : Terraform
* Cloud : GCP
* Orchestration: Prefect
* DL: GCS
* DW: BigQuery
* Transformations: dbt


### First steps 

For this project, we'll use a free trial version of google cloud platform. 

1. Create an account with your Google email ID 
2. Setup your first [project](https://console.cloud.google.com/) if you haven't already
3. Setup [service account & authentication](https://cloud.google.com/docs/authentication/getting-started) for this project
    * Grant `Viewer` role to begin with.
    * Download service-account-keys (.json) for auth.
4. Download [SDK](https://cloud.google.com/sdk/docs/quickstart) for local setup
5. Set environment variable to point to your downloaded GCP keys:
   ```shell
   export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
   # Refresh token/session, and verify authentication
   gcloud auth application-default login
   ```
   
### Setup for Access
 
1. [IAM Roles](https://cloud.google.com/storage/docs/access-control/iam-roles) for Service account:
   * Go to the *IAM* section of *IAM & Admin* https://console.cloud.google.com/iam-admin/iam
   * Click the *Edit principal* icon for your service account.
   * Add these roles in addition to *Viewer* : **Storage Admin** + **Storage Object Admin** + **BigQuery Admin**
   
2. Enable these APIs for your project:
   * https://console.cloud.google.com/apis/library/iam.googleapis.com
   * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
   
3. Please ensure `GOOGLE_APPLICATION_CREDENTIALS` env-var is set.
   ```shell
   export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
   ```


### 1. Startup cloud services

Access the './terraform' directory and run the below commands, this commandas will start Google cloud storage and Bigquery services nedeed to pipeline works.
 #### Pre-Requisites
1. Terraform client installation: https://www.terraform.io/downloads
### Execution

```shell
# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=<your-gcp-project-id>"
```

```shell
# Create new infra
terraform apply -var="project=<your-gcp-project-id>"
```

```shell
# Delete infra after your work, to avoid costs on any running services
terraform destroy
```

### 2. Requirements

Install python libs to run scripts correctly.

```shell
# This command nedeed be run on ./
pip install -r requirements.txt
```

### 3. Scripts orchestration

To do this we use Prefect, run below commands:
```shell
# Start the GUI and access on http://127.0.0.1:4200/
prefect orion start
```
```shell
# Start the Prefect agent on default
prefect agent start -q 'default'
```
```shell
# Realize the first script deploy with schedule for 01:00 AM for UTC -3
prefect deployment build 01_etl_chalengers_to_gcs.py:etl_chalengers_to_gcs --name 01_chalengers_to_gcs --cron '0 1 * * *' --timezone 'America/Sao_Paulo' -a
```
```shell
# Realize the second deploy with schedule for 01:30 AM for UTC -3
prefect deployment build 02_etl_match_ids_to_gcs.py:etl_match_ids_to_gcs_subflow --name 02_match_ids_to_gcs --cron '30 1 * * *' --timezone 'America/Sao_Paulo' -a
```
```shell
# Realize the thirty deploy with schedule for 03:00 AM for UTC -3
prefect deployment build 03_etl_data_match_to_bq.py:etl_data_match_to_bq_subflow --name 03_data_match_to_bq --cron '0 3 * * *' --timezone 'America/Sao_Paulo' -a
```
These scripts will be run every day below the chalenger queue resets, assim capturando as partidas dos novos jogadores que entraram no chalenger.

### 4. dbt/bigquery transformations
 Após o passo três o fluxo de dados irá gerar uma tabela chamada 'match_data_raw' no bigquery, a partir dela realizaremos transformações com o dbt e posteriormente o particionamento e clusterização da mesma com o bigquery.
 #### dbt
 O repositório do projeto dbt se encontra em './dbt' ou acesse clicando [aqui](./dbt/).
 Foram utilizados 4 models para transformações com o dbt:
 * stg_data_match: Realiza transformações nas tipagens dos dados, exceto em timestamps.
 * prep_data_match: Realiza transformações nas tipagens dos dados, somente e timestamps.
 * row_numbers: Realiza um row_numbers() para identificar os registros duplicados.
 * refine_data_match: Adiciona uma coluna 'delete_case' que recebe não para o primeiro registro particionado pelo modelo row_numbers() e 'sim' para o restante

#### bigquery
Neste momento temos uma tabela denominada "refine_data_match" com os dados duplicados sinalizados e com as tipagens corretas. A partir disso iremos particionar e clusterizar os dados realizando alguns filtros.
Rode no bigquery o script presente no caminho './big_query/partitioning_and_clustering.sql', onde iremos particionar a tabela pela data de criação das partidas e clusterizar pelo nome dos personagens, dessa forma otimizaremos as consultas realizadas com o intuito de filtrar os campões jogados em certo periodo.
```shell
CREATE OR REPLACE TABLE `lol-data-project-383712.lol.match_data_clean`
PARTITION BY DATE(gameCreation)
CLUSTER BY championName AS
SELECT * FROM `lol-data-project-383712.dbt_lol_project.refine_data_match` WHERE delete_case = 'nao' AND game_mode = 'CLASSIC';
```
### 5. Data visualization with google data studio
In this step you need to connect your table in bigquery with your google data studio account(tutorial [here](https://support.google.com/looker-studio/answer/6295968?hl=en#zippy=%2Cin-this-article)).
Done this, you can elaborate a dashboard with data.
![project dashboard](project_architecture.drawio.png"project dashboard")







