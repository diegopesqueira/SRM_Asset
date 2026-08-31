from datetime import datetime
from airflow import DAG
from airflow.models import Param
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='03_carga_estabelecimentos_bronze',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    # 1. Definição dos parâmetros padrão e aceitos pela DAG
    params={
        "ano_mes": Param(
            default="2026-03",
            type="string",
            description="Ano e mês no formato YYYY-MM"
        ),
        "caminho_origem": Param(
            default="/opt/airflow/dados_temp/unzip/Estabelecimentos_2026-03",
            type="string",
            description="Caminho completo do diretório/arquivo descomprimido"
        ),
    },
) as dag:

    executar_carga_bronze_estabelecimentos = SparkSubmitOperator(
        task_id='executar_carga_bronze_estabelecimentos',
        application='/opt/airflow/receita_spark/carga_estabelecimentos_bronze.py',
        conn_id='spark_default',
        total_executor_cores='2',
        executor_cores='1',
        executor_memory='1g',
        driver_memory='1g',
        name='carga_estabelecimentos_bronze',
        # 2. Resgate dinâmico usando Jinja Templating do Airflow
        application_args=[
            "{{ params.ano_mes }}",
            "{{ params.caminho_origem }}"
        ],
    )