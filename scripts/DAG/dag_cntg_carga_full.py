import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/receita")

from receita.minio_client import MinIOClient
from receita.estabelecimentos_bronze import CargaEstabelecimentoBronze
from receita.municipios_bronze import CargaMunicipiosBronze
from receita.estabelecimentos_silver import CargaEstabelecimentosSilver
from receita.municipios_silver import CargaMunicipiosSilver
from receita.matriz_filial_gold import CargaMatrizFilialGold
from receita.dashboard import GeraDashboard
from receita.download_arquivos import Download
from receita.unzip_files import UnzipFiles


def executa_download(**context):
    dag_run_conf = context.get("dag_run").conf or {}
    mes_informado = dag_run_conf.get("reference_month") or context["params"].get("reference_month")
    mes_ref = mes_informado if mes_informado else context["logical_date"].strftime("%Y-%m")
    download_dir = context["params"].get("download_dir", "/opt/airflow/dados_temp/zip")

    print(f"[DAG] Iniciando download dos arquivos para o mês: {mes_ref}")
    downloader = Download(reference_month=mes_ref, output_dir=download_dir)
    downloader.run()


def executa_unzip(**context):
    dag_run_conf = context.get("dag_run").conf or {}
    mes_informado = dag_run_conf.get("reference_month") or context["params"].get("reference_month")
    mes_ref = mes_informado if mes_informado else context["logical_date"].strftime("%Y-%m")
    download_dir = context["params"].get("download_dir", "/opt/airflow/dados_temp/zip")
    output_dir = context["params"].get("output_dir", "/opt/airflow/dados_temp/unzip")

    print(f"[DAG] Iniciando descompactação dos arquivos para o mês: {mes_ref}")
    unzipper = UnzipFiles(reference_month=mes_ref, zip_directory=download_dir, output_directory=output_dir)
    unzipper.run()


def executa_carga_estabelecimentos_bronze(**context):
    dag_run_conf = context.get("dag_run").conf or {}
    mes_informado = dag_run_conf.get("reference_month") or context["params"].get("reference_month")
    mes_ref = mes_informado if mes_informado else context["logical_date"].strftime("%Y-%m")
    input_dir = context["params"].get("output_dir", "/opt/airflow/dados_temp/unzip")

    print(f"[DAG] Iniciando carga bronze de estabelecimentos para o mês: {mes_ref}")
    minio_client = MinIOClient(bucket_name="receita")
    carga = CargaEstabelecimentoBronze(
        input_directory=input_dir,
        reference_month=mes_ref,
        minio_client=minio_client,
    )
    carga.run()


def executa_carga_municipios_bronze(**context):
    dag_run_conf = context.get("dag_run").conf or {}
    mes_informado = dag_run_conf.get("reference_month") or context["params"].get("reference_month")
    mes_ref = mes_informado if mes_informado else context["logical_date"].strftime("%Y-%m")
    input_dir = context["params"].get("output_dir", "/opt/airflow/dados_temp/unzip")

    print(f"[DAG] Iniciando carga bronze de municípios para o mês: {mes_ref}")
    minio_client = MinIOClient(bucket_name="receita")
    carga = CargaMunicipiosBronze(
        input_directory=input_dir,
        reference_month=mes_ref,
        minio_client=minio_client,
    )
    carga.run()


def executa_carga_estabelecimentos_silver(**context):
    dag_run_conf = context.get("dag_run").conf or {}
    mes_informado = dag_run_conf.get("reference_month") or context["params"].get("reference_month")
    mes_ref = mes_informado if mes_informado else context["logical_date"].strftime("%Y-%m")

    print(f"[DAG] Iniciando carga silver de estabelecimentos para o mês: {mes_ref}")
    minio_client = MinIOClient(bucket_name="receita")
    carga = CargaEstabelecimentosSilver(
        reference_month=mes_ref,
        minio_client=minio_client,
    )
    carga.run()


def executa_carga_municipios_silver(**context):
    dag_run_conf = context.get("dag_run").conf or {}
    mes_informado = dag_run_conf.get("reference_month") or context["params"].get("reference_month")
    mes_ref = mes_informado if mes_informado else context["logical_date"].strftime("%Y-%m")

    print(f"[DAG] Iniciando carga silver de municípios para o mês: {mes_ref}")
    minio_client = MinIOClient(bucket_name="receita")
    carga = CargaMunicipiosSilver(
        reference_month=mes_ref,
        minio_client=minio_client,
    )
    carga.run()


def executa_carga_matriz_filial_gold(**context):
    dag_run_conf = context.get("dag_run").conf or {}
    mes_informado = dag_run_conf.get("reference_month") or context["params"].get("reference_month")
    mes_ref = mes_informado if mes_informado else context["logical_date"].strftime("%Y-%m")

    print(f"[DAG] Iniciando carga gold de matriz/filial para o mês: {mes_ref}")
    minio_client = MinIOClient(bucket_name="receita")
    carga = CargaMatrizFilialGold(
        reference_month=mes_ref,
        minio_client=minio_client,
    )
    carga.run()


def executa_geracao_dashboard(**context):
    dag_run_conf = context.get("dag_run").conf or {}
    mes_informado = dag_run_conf.get("reference_month") or context["params"].get("reference_month")
    mes_ref = mes_informado if mes_informado else context["logical_date"].strftime("%Y-%m")
    output_dir = context["params"].get("dashboard_dir", "dashboard")

    print(f"[DAG] Iniciando geração do dashboard para o mês: {mes_ref}")
    minio_client = MinIOClient(bucket_name="receita")
    dashboard = GeraDashboard(
        reference_month=mes_ref,
        minio_client=minio_client,
        output_dir=output_dir,
    )
    dashboard.run()


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="100_pipeline_consolidado_receita_federal",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    doc_md="""
    ### DAG Consolidada: Pipeline Completo da Receita Federal
    Executa o fluxo completo ponta a ponta utilizando PythonOperator para todas as etapas 
    (Download, Unzip, Bronze, Silver, Gold e Geração de Dashboard).
    """,
    params={
        "reference_month": Param(
            default="2025-12",
            type="string",
            description="Mês de referência no formato YYYY-MM",
        ),
        "download_dir": Param(
            default="/opt/airflow/dados_temp/zip",
            type="string",
            description="Diretório onde os arquivos .zip serão salvos",
        ),
        "output_dir": Param(
            default="/opt/airflow/dados_temp/unzip",
            type="string",
            description="Diretório de saída para os CSVs extraídos",
        ),
        "dashboard_dir": Param(
            default="dashboard",
            type="string",
            description="Diretório local temporário para gerar a imagem do dashboard",
        ),
    },
) as dag:

    task_download = PythonOperator(
        task_id="1_Download",
        python_callable=executa_download,
    )

    task_unzip = PythonOperator(
        task_id="2_Unzip",
        python_callable=executa_unzip,
    )

    task_estabelecimentos_bronze = PythonOperator(
        task_id="3_Carga_Estabelecimentos_Bronze",
        python_callable=executa_carga_estabelecimentos_bronze,
    )

    task_municipios_bronze = PythonOperator(
        task_id="4_Carga_Municipios_Bronze",
        python_callable=executa_carga_municipios_bronze,
    )

    task_estabelecimentos_silver = PythonOperator(
        task_id="5_Carga_Estabelecimentos_Silver",
        python_callable=executa_carga_estabelecimentos_silver,
    )

    task_municipios_silver = PythonOperator(
        task_id="6_Carga_Municipios_Silver",
        python_callable=executa_carga_municipios_silver,
    )

    task_gold = PythonOperator(
        task_id="7_Carga_Matriz_Filial_Gold",
        python_callable=executa_carga_matriz_filial_gold,
    )

    task_gera_dashboard = PythonOperator(
        task_id="8_Gera_Dashboard",
        python_callable=executa_geracao_dashboard,
    )

    # Definição das dependências do pipeline
    task_download >> task_unzip
    task_unzip >> [task_estabelecimentos_bronze, task_municipios_bronze]
    task_estabelecimentos_bronze >> task_estabelecimentos_silver
    task_municipios_bronze >> task_municipios_silver
    [task_estabelecimentos_silver, task_municipios_silver] >> task_gold
    task_gold >> task_gera_dashboard