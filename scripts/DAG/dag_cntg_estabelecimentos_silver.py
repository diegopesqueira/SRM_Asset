from datetime import datetime, timedelta
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# Garante que as importações encontrem os módulos corretamente
sys.path.append("/opt/airflow/receita")
sys.path.append("/opt/airflow/receita_spark")

from receita.minio_client import MinIOClient
from receita.estabelecimentos_silver import CargaEstabelecimentosSilver


def executa_carga_estabelecimentos_silver(**context):
    # O valor preenchido na interface web (UI) fica armazenado em context["params"]
    mes_ref = context["params"].get("reference_month")

    # Fallback de segurança para a data de execução, caso o parâmetro venha vazio
    if not mes_ref:
        mes_ref = context["logical_date"].strftime("%Y-%m")

    print(f"[DAG] Iniciando carga silver de estabelecimentos para o mês: {mes_ref}")

    minio_client = MinIOClient(bucket_name="receita")
    carga = CargaEstabelecimentosSilver(
        reference_month=mes_ref,
        minio_client=minio_client,
    )
    carga.run()


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="102_carga_estabelecimentos_silver_contingencia",
    default_args=default_args,
    description="DAG para execução da camada silver de estabelecimentos com Python puro",
    schedule_interval=None,  # Definido como None para acionamento manual via UI
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "reference_month": Param(
            default="2025-12",
            type="string",
            description="Mês de referência no formato YYYY-MM"
        )
    },
) as dag:

    task_executar_silver = PythonOperator(
        task_id="executar_carga_estabelecimentos_silver",
        python_callable=executa_carga_estabelecimentos_silver,
    )