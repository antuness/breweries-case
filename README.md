# Breweries Case

## Objetivo
O objetivo deste case é consumir dados da API Open Brewery DB, transformá-los e persistir em um data lake, seguindo a arquitetura de medallion com três camadas: Bronze, Silver e Gold.

## Arquitetura do Data Lake
- **Bronze**: Armazena os dados brutos da API em formato JSON, organizados por data de ingestão.
- **Silver**: Transforma os dados para formato Parquet, com normalização de colunas e particionamento por estado.
- **Gold**: Cria visão analítica agregada, contabilizando a quantidade de cervejarias por tipo e por estado.

## Tecnologias Utilizadas
- **Orquestração**: Apache Airflow
- **Linguagem**: Python 3.11
- **Containerização**: Docker + Docker Compose
- **Banco de Metadados**: PostgreSQL
- **Formato de Armazenamento**: JSON (Bronze) e Parquet (Silver e Gold)

## Estrutura de Pastas
```
.
├── app
│   ├── ingestion
│   │   └── fetch_breweries.py
│   ├── transforms
│   │   ├── transform_silver.py
│   │   └── aggregate_gold.py
│   └── quality
│       └── run_quality_checks.py
├── airflow
│   └── dags
│       └── breweries_pipeline.py
├── data_lake
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docker-compose.yml
├── Dockerfile
└── README.md
```

## Fluxo da Pipeline
- **Ingestão (Bronze)**: 
  - Consome a API `https://api.openbrewerydb.org/v1/breweries?per_page=200`.
  - Persiste em `data_lake/bronze/ingestion_date=YYYY-MM-DD/breweries.json`.
- **Transformação (Silver)**:
  - Seleciona e normaliza colunas: `id, name, brewery_type, city, state, country`.
  - Particiona em Parquet por estado.
- **Qualidade**:
  - Valida colunas obrigatórias.
  - Garante unicidade de IDs e brewery_types válidos.
- **Agregação (Gold)**:
  - Conta quantidade de cervejarias por estado e por tipo.
  - Salva em `data_lake/gold/exec_date=YYYY-MM-DD/brewery_counts.parquet`.

## Execução
- Subir os containers:
  ```bash
  docker compose up -d --build
  ```
- Acessar o Airflow em: [http://localhost:8080](http://localhost:8080)  
  Usuário: `` | Senha: ``

- Ativar e rodar a DAG `breweries_pipeline`.

## Monitoramento e Alertas
- Logs disponíveis diretamente no Airflow.
- Qualidade de dados implementada em `app/quality/run_quality_checks.py`.
- Possibilidade de integração com Slack via variável de ambiente `SLACK_WEBHOOK_URL`.

## Repositório
Este projeto deve ser versionado em GitHub, incluindo:
- Código Python
- Definições de DAGs
- Dockerfile e docker-compose.yml
- Este `README.md`