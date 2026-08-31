import sys
import traceback
from schema import SCHEMA_ESTABELECIMENTOS
from spark_session import get_spark_session


def processar_estabelecimentos_bronze(reference_month: str, input_dir: str) -> None:
    try:
        spark = get_spark_session("Bronze_Estabelecimentos")
        output_path = f"s3a://receitas/bronze/estabelecimentos/year_month={reference_month}"

        print(f"[START] Lendo CSVs de Estabelecimentos do diretório: {input_dir}")

        # Aponta para a pasta correta com coringa glob
        caminho_leitura = f"{input_dir}/*"

        df_bruto = (
            spark.read.option("delimiter", ";")
            .option("encoding", "ISO-8859-1")
            .option("maxPartitionBytes", 134217728)
            .schema(SCHEMA_ESTABELECIMENTOS)
            .csv(caminho_leitura)
        )

        df_bruto.repartition(20).write.mode("overwrite").parquet(output_path)
        print(f"[OK] Bronze Estabelecimentos gravado em: {output_path}")

    except Exception as e:
        print("=== ERRO CRITICO NO SPARK ===")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    mes = sys.argv[1] if len(sys.argv) > 1 else "2026-03"
    dir_csv = sys.argv[2] if len(sys.argv) > 2 else f"/opt/airflow/dados_temp/unzip/Estabelecimentos_{mes}"
    processar_estabelecimentos_bronze(mes, dir_csv)