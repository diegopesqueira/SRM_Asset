from datetime import datetime, timedelta
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

sys.path.append("/opt/airflow/receita")

from receita.minio_client import MinIOClient
from receita.municipios_bronze import CargaMunicipiosBronze


def executa_carga_municipios_bronze(**context):
    dag_run_conf = context.get("dag_run").conf or {}
    mes_informado = dag_run_conf.get("mes_ref") or context["params"].get("mes_ref")

    mes_ref = mes_informado if mes_informado else context["logical_date"].strftime("%Y-%m")
    input_dir = "/opt/airflow/dados_temp/unzip"

    print(f"[DAG] Iniciando carga bronze de municípios para o mês: {mes_ref}")

    minio_client = MinIOClient(bucket_name="receita")
    carga = CargaMunicipiosBronze(
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
    dag_id="103_carga_municipios_bronze_contingencia",
    default_args=default_args,
    description="DAG para execução da camada bronze de municípios com Python puro",
    schedule_interval="@monthly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "mes_ref": Param("2025-12", type="string", description="Mês de referência no formato YYYY-MM")
    },
) as dag:

    task_executar_municipios_bronze = PythonOperator(
        task_id="executa_carga_municipios_bronze",
        python_callable=executa_carga_municipios_bronze,
    )