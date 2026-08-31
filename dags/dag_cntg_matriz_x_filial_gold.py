from datetime import datetime, timedelta
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/receita_spark")

from receita.minio_client import MinIOClient
from receita.matriz_filial_gold import CargaMatrizFilialGold


def executa_carga_matriz_filial_gold(**context):
    mes_ref = context["logical_date"].strftime("%Y-%m")

    print(f"[DAG] Iniciando carga gold de matriz/filial para o mês: {mes_ref}")

    minio_client = MinIOClient()
    carga = CargaMatrizFilialGold(
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
    dag_id="105_carga_matriz_filial_gold_contingencia",
    default_args=default_args,
    description="DAG para execução da camada gold de matriz/filial com Python puro",
    schedule_interval="@monthly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    task_executar_gold = PythonOperator(
        task_id="executa_carga_matriz_filial_gold",
        python_callable=executa_carga_matriz_filial_gold,
    )