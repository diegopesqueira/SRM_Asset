import sys
from receita_spark.spark_session import get_spark_session


def processar_municipios_bronze(reference_month: str, input_csv_path: str) -> None:
    spark = get_spark_session("Bronze_Municipios")
    output_path = f"s3a://receitas/bronze/municipios/year_month={reference_month}"

    print(f"[START] Lendo CSV de Municípios de: {input_csv_path}")

    df_bruto = (
        spark.read.option("delimiter", ";")
        .option("encoding", "ISO-8859-1")
        .csv(input_csv_path)
        .toDF("codigo", "descricao")
    )

    df_bruto.write.mode("overwrite").parquet(output_path)
    print(f"[OK] Bronze Municípios gravado com sucesso em: {output_path}")


if __name__ == "__main__":
    mes = sys.argv[1] if len(sys.argv) > 1 else "2026-03"

    # Caminho padrão ajustado para a estrutura de pastas gerada pelo unzip (*.csv lê todos dentro do diretório)
    path_csv = (
        sys.argv[2]
        if len(sys.argv) > 2
        else f"/opt/airflow/dados_temp/unzip/Municipios_{mes}/*.csv"
    )

    processar_municipios_bronze(mes, path_csv)