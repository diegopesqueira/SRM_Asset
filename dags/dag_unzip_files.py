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
    dag_id="02_descompacta_arquivos",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md="""
    ### DAG: Unzip e Padronização dos Arquivos da Receita Federal
    Descompacta os arquivos `.zip`, renomeia e organiza por entidade e data de referência.
    """,
    params={
        "reference_month": Param(
            default="2025-12",
            type="string",
            description="Mês de referência no formato YYYY-MM",
        ),
        "zip_dir": Param(
            default="/opt/airflow/dados_temp/zip",
            type="string",
            description="Diretório onde estão os arquivos .zip baixados",
        ),
        "output_dir": Param(
            default="/opt/airflow/dados_temp/unzip",
            type="string",
            description="Diretório de saída para os CSVs extraídos",
        ),
    },
) as dag:

    task_unzip = BashOperator(
        task_id="executar_unzip",
        bash_command=(
            "python /opt/airflow/receita_spark/unzip_files.py "
            "{{ params.zip_dir }} "
            "{{ params.reference_month }} "
            "{{ params.output_dir }}"
        ),
    )

    task_unzip