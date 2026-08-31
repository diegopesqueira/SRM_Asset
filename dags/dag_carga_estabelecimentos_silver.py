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
    dag_id="05_carga_estabelecimentos_silver",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md="""
    ### DAG: Carga Silver de Estabelecimentos
    Lê os dados Parquet da camada Bronze no MinIO, aplica filtros de negócio (Município 7107 e Situação 2), 
    trata valores nulos, formata campos como CNPJ completo e grava o resultado particionado na camada Silver.
    """,
    params={
        "reference_month": Param(
            default="2025-12",
            type="string",
            description="Mês de referência no formato YYYY-MM",
        ),
    },
) as dag:

    task_carga_silver = BashOperator(
        task_id="executar_carga_silver_estabelecimentos",
        bash_command=(
            "python /opt/airflow/receita_spark/carga_estabelecimentos_silver.py "
            "{{ params.reference_month }}"
        ),
    )

    task_carga_silver