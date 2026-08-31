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
    dag_id="06_carga_municipios_silver",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md="""
    ### DAG: Processamento Silver de Municípios via PySpark
    Lê a camada Bronze no MinIO/S3, realiza o cast do código para integer,
    remove espaços em branco da descrição e elimina duplicatas.
    """,
    params={
        "reference_month": Param(
            default="2025-12",
            type="string",
            description="Mês de referência no formato YYYY-MM",
        ),
    },
) as dag:

    task_spark_silver_municipios = BashOperator(
        task_id="executar_spark_silver_municipios",
        bash_command=(
            "python /opt/airflow/receita_spark/carga_municipios_silver.py "
            "{{ params.reference_month }}"
        ),
    )

    task_spark_silver_municipios