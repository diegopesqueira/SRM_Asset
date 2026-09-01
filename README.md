# SRM_Asset

# Pipeline de Ingestão e Processamento - Dados da Receita Federal (Lakehouse com MinIO e Spark)

## **Contexto do Desafio**

Este repositório apresenta a solução desenvolvida para o **Desafio Técnico de Engenharia de Dados**, cujo objetivo é analisar os Dados Abertos do CNPJ da Receita Federal para responder à seguinte pergunta de negócio do time de Inteligência de Mercado:

> *"Quantas filiais vs. matrizes com Situação Cadastral 'Ativa' existem na cidade de São Paulo?"*

### **Atendimento aos Requisitos Técnicos**
* **Arquitetura Medallion:** Dados segregados e processados estruturadamente nas camadas *Bronze* (Landing), *Silver* (Processing/Filtros por São Paulo e Situação Ativa) e *Gold* (Curated/Agregações finais).
* **Pipeline Parametrizável & Idempotente:** Orquestração via Apache Airflow permitindo reexecuções seguras sem duplicidade de dados.
* **Reprodutibilidade:** Ambiente totalmente conteinerizado utilizando Docker Compose (Airflow, Spark, MinIO).

Pipeline orquestrado via **Apache Airflow** rodando em **Docker**, responsável por extrair, processar e carregar dados de municípios e estabelecimentos utilizando processamento distribuído (PySpark) e armazenamento em object storage (**MinIO**).

---

## **Arquitetura do Projeto**

```text
.
├── dags/
│   └── dag_cntg_municipios_silver.py              # DAG principal do Airflow
├── receita/                                       
│   ├── __init__.py                                
│   ├── dashboard.py                               # Script de geração do dashboard
│   ├── descompacta_arquivo.py                     # Script Unzip de Arquivos
│   ├── download_arquivos.py                       # Script baixa arquivos do site da Receita Federal
│   ├── estabelecimentos_bronze.py                 # Script de carga estabelecimentos para camada Bronze
│   ├── estabelecimentos_silver.py                 # Script de carga estabelecimentos para camada Silver
│   ├── executa_receita_federal.py                 # Script Execução consolidada
│   ├── helpers.py                                 # Script de Normalização e aplicação de Schema
│   ├── matriz_filial_gold.py                      # Script de carga consolidação camada gold
│   ├── minio_client.py                            # Cliente de integração com o MinIO
│   ├── municipios_bronze.py                       # Script de carga municipios para camada bronze
│   ├── municipios_silver.py                       # Script de carga municipios para camada Silver
│   └── schema.py                                  # Script com nome das colunas
│                                                  
├── receita_spark/                                 # Scripts de processamento com PySpark
│   ├── __init__.py
│   ├── carga_estabelecimentos_bronze.py           # Script de carga estabelecimentos para camada Bronze
│   ├── carga_estabelecimentos_silver.py           # Script de carga estabelecimentos para camada Silver
│   ├── carga_gold.py                              # Script de carga consolidação camada gold
│   ├── carga_municipios_bronze.py                 # Script de carga municipios para camada bronze
│   ├── carga_municipios_silver.py                 # Script de carga municipios para camada Silver
│   ├── dashboard.py                               # Script de geração do dashboard
│   ├── download_arquivos.py                       # Script baixa arquivos do site da Receita Federal
│   ├── helpers.py                                 # Script de Normalização e aplicação de Schema
│   ├── minio_client.py                            # Cliente de integração com o MinIO
│   ├── schema.py                                  # Script com nome das colunas
│   ├── spark_session.py                           # Script de criação de Sessão do Spark
│   └── unzip_files.py                             # Script Unzip de Arquivos
├── dados_temp/                                    # Diretório de arquivos temporários
├── Dockerfile.airflow                             # Imagem customizada do Airflow
├── docker-compose.yaml                            # Orquestração dos containers
└── requirements.txt                               # Dependências Python do projeto
```

## **Como Subir o Ambiente**
1. Clone o repositório e acesse a pasta raiz do projeto.
   ```
   ├── git clone https://github.com/diegopesqueira/SRM_Asset.git
   ```

2. docker compose up -d --build

3. Copie os scripts.py para as respectivas pastas: 
   ```
   receita_spark
   ├── docker cp <diretório>/Git/scripts/receita_spark/. airflow-scheduler:/opt/airflow/receita_spark
   └── docker cp <diretório>/Git/scripts/receita_spark/. airflow-webserver:/opt/airflow/receita_spark
   receita
   ├── docker cp <diretório>/Git/scripts/receita/. airflow-scheduler:/opt/airflow/receita
   └── docker cp <diretório>/Git/scripts/receita/. airflow-webserver:/opt/airflow/receita
   DAGs
   ├── docker cp <diretório>/Git/scripts/DAG/. airflow-scheduler:/opt/airflow/dags
   └── docker cp <diretório>/Git/scripts/DAG/. airflow-webserver:/opt/airflow/dags
   ```

4. Os softwares estão configurados nas seguintes portas: 
   ```
   minIO   -> http://localhost:9001/
   spark   -> http://localhost:8080/
   airflow -> http://localhost:8081/
   ```

5. As Dags estão classificadas por ordem de execução

### Python
```
Ordem de Execução:
├── 01_download_arquivos_receita_federal
├── 02_descompacta_arquivos
├── 101_carga_estabelecimentos_bronze_contingencia
├── 102_carga_estabelecimentos_silver_contingencia
├── 103_carga_municipios_bronze_contingencia
├── 104_carga_municipios_silver_contingencia
├── 105_carga_matriz_filial_gold_contingencia
└── 106_gera_dashboard

Executar Pipeline Completo
├── 100_pipeline_consolidado_receita_federal
```

###Pyspark
```
Ordem de Execução:
├── 01_download_arquivos_receita_federal
├── 02_descompacta_arquivos
├── 03_carga_estabelecimentos_bronze
├── 04_carga_municipios_bronze
├── 05_carga_estabelecimentos_silver
├── 06_carga_municipios_silver
├── 07_carga_gold_matriz_x_filial
└── 08_gera_dashboard
```

6. Os arquivos estão disponibilizados no minIO através do link informado acima.

7. Senhas de acesso: 
   
   airflow: 
      user: admin
	  password: admin
   
   minIO: 
      user: admin
	  password: admin123
