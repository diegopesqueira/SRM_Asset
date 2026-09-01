import sys
from pyspark.sql.functions import col
from receita_spark.spark_session import get_spark_session


def processar_estabelecimentos_silver(reference_month: str) -> None:
    spark = get_spark_session("Silver_Estabelecimentos")
    input_path = f"s3a://receitas/bronze/estabelecimentos/year_month={reference_month}"
    output_path = f"s3a://receitas/silver/estabelecimentos/year_month={reference_month}"

    df_bronze = spark.read.parquet(input_path)

    df_silver = (
        df_bronze.withColumn("codigo_municipio", col("municipio").cast("integer"))
        .withColumn("situacao_cadastral", col("situacao_cadastral").cast("integer"))
        .withColumn("matriz_filial", col("identificador_matriz_filial").cast("integer"))
        .filter(col("situacao_cadastral") == 2)  # 02 = Ativa
    )

    df_silver.write.mode("overwrite").parquet(output_path)
    print(f"[OK] Silver Estabelecimentos gravado com sucesso em: {output_path}")


if __name__ == "__main__":
    mes = sys.argv[1] if len(sys.argv) > 1 else "2025-12"
    processar_estabelecimentos_silver(mes)