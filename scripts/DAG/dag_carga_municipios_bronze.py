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
    dag_id="04_carga_municipios_bronze",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    doc_md="""
    ### DAG: Processamento Bronze de Municípios via PySpark
    Lê o arquivo CSV descompactado de Municípios, aplica o encoding ISO-8859-1 e delimitador `;`,
    define a estrutura básica de colunas (`codigo`, `descricao`) e grava em Parquet no S3/MinIO.
    """,
    params={
        "reference_month": Param(
            default="2025-12",
            type="string",
            description="Mês de referência no formato YYYY-MM",
        ),
        "input_csv_path": Param(
            default="/opt/airflow/dados_temp/unzip/Municipios_2025-12/municipios_2025-12_part_001.csv",
            type="string",
            description="Caminho completo do arquivo CSV extraído de Municípios",
        ),
    },
) as dag:

    task_spark_bronze_municipios = BashOperator(
        task_id="executar_spark_bronze_municipios",
        bash_command=(
            "python /opt/airflow/receita_spark/carga_municipios_bronze.py "
            "{{ params.reference_month }} "
            "{{ params.input_csv_path }}"
        ),
    )

    task_spark_bronze_municipios