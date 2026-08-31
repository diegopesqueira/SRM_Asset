from datetime import datetime
from airflow import DAG
from airflow.models import Param
from airflow.operators.bash import BashOperator

# Definição dos parâmetros padrão da DAG
default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="01_download_arquivos_receita_federal",
    default_args=default_args,
    schedule_interval=None,  # Execução manual ou disparada por outra DAG
    catchup=False,
    doc_md="""
    ### DAG: Downloader de Arquivos da Receita Federal
    Faz o download dos arquivos em formato `.zip` dos Estabelecimentos e Municípios.

    **Parâmetros:**
    * `reference_month`: Mês de referência no formato `YYYY-MM` (ex: `2025-12`).
    """,
    params={
        "reference_month": Param(
            default="2025-12",
            type="string",
            description="Mês de referência dos dados no formato YYYY-MM",
        ),
        "download_dir": Param(
            default="/opt/airflow/dados_temp/zip",
            type="string",
            description="Diretório onde os arquivos .zip serão salvos",
        ),
    },
) as dag:
    # Task para executar o script Python via linha de comando
    task_download = BashOperator(
        task_id="executar_download",
        bash_command=(
            "python /opt/airflow/receita_spark/download_arquivos.py "
            "{{ params.reference_month }} "
            "{{ params.download_dir }}"
        ),
    )

    task_download