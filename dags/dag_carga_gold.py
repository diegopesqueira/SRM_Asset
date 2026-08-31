from datetime import datetime
from airflow import DAG
from airflow.models import Param
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="07_carga_gold_matriz_x_filial",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md="""
    ### DAG: Processamento da Camada Gold (Matriz x Filial) via PySpark
    Lê os dados das tabelas Silver de Estabelecimentos e Municípios no MinIO/S3, 
    filtra a cidade de São Paulo, classifica os estabelecimentos por tipo (Matriz/Filial) 
    e consolida os totais agregados na camada Gold.
    """,
    params={
        "reference_month": Param(
            default="2025-12",
            type="string",
            description="Mês de referência no formato YYYY-MM",
        ),
    },
) as dag:

    task_spark_gold = BashOperator(
        task_id="executar_spark_gold",
        bash_command=(
            "python /opt/airflow/receita_spark/carga_gold.py "
            "{{ params.reference_month }}"
        ),
    )

    task_spark_gold