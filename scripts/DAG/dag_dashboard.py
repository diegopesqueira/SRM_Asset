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
    dag_id="08_gera_dashboard",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md="""
    ### DAG: Geração do Dashboard em Imagem (Plotly)
    Lê os dados agregados da Camada Gold no MinIO, gera a imagem estática do gráfico em pizza 
    e faz o upload do arquivo gerado de volta para o bucket do MinIO.
    """,
    params={
        "reference_month": Param(
            default="2025-12",
            type="string",
            description="Mês de referência no formato YYYY-MM",
        ),
    },
) as dag:

    task_gera_dashboard = BashOperator(
        task_id="executar_geracao_dashboard",
        bash_command=(
            "python /opt/airflow/receita/dashboard.py "
            "{{ params.reference_month }}"
        ),
    )

    task_gera_dashboard