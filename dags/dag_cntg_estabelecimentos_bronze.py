from datetime import datetime, timedelta
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/receita_spark")

from receita.minio_client import MinIOClient
from receita.estabelecimentos_bronze import CargaEstabelecimentoBronze


def executa_carga_estabelecimento_bronze(**context):
    mes_ref = context["logical_date"].strftime("%Y-%m")
    input_dir = "/opt/airflow/dados_temp/unzip"

    print(f"[DAG] Iniciando carga bronze para o mês: {mes_ref}")

    minio_client = MinIOClient()
    carga = CargaEstabelecimentoBronze(
        input_directory=input_dir,
        reference_month=mes_ref,
        minio_client=minio_client,
    )
    carga.run()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="101_carga_estabelecimentos_bronze_contingencia",
    default_args=default_args,
    description="DAG para execução da carga bronze de estabelecimentos com Python puro",
    schedule_interval="@monthly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    task_executar_carga = PythonOperator(
        task_id="executar_carga_estabelecimentos",
        python_callable=executa_carga_estabelecimento_bronze,
    )