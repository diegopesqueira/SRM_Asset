import sys
from pyspark.sql.functions import col, count, when
from receita_spark.spark_session import get_spark_session


def processar_gold(reference_month: str) -> None:
    spark = get_spark_session("Gold_Matriz_Filial")
    bucket = "receitas"

    estab_path = f"s3a://{bucket}/silver/estabelecimentos/year_month={reference_month}"
    muni_path = f"s3a://{bucket}/silver/municipios/year_month={reference_month}"
    output_path = f"s3a://{bucket}/gold/matriz_filial/year_month={reference_month}"

    print(f"[START] Processando Camada Gold para a referência: {reference_month}")

    # Leitura dos Parquets da Camada Silver
    df_estab = spark.read.parquet(estab_path)
    df_muni = spark.read.parquet(muni_path)

    # Filtra o município de São Paulo
    df_sp = df_muni.filter(col("nome_municipio") == "SAO PAULO")

    # Join entre Estabelecimentos (Ativos) e Municípios
    df_joined = df_estab.join(
        df_sp,
        df_estab["codigo_municipio"] == df_sp["codigo_municipio"],
        "inner",
    )

    # Classificação em Matriz / Filial e Agregação
    df_gold = (
        df_joined.withColumn(
            "tipo_estabelecimento",
            when(col("matriz_filial") == 1, "Matriz")
            .when(col("matriz_filial") == 2, "Filial")
            .otherwise("Outros"),
        )
        .groupBy("tipo_estabelecimento", "nome_municipio")
        .agg(count("*").alias("qtd_estabelecimentos"))
    )

    df_gold.write.mode("overwrite").parquet(output_path)
    print(f"[OK] Gold Matriz/Filial gerada com sucesso em: {output_path}")


if __name__ == "__main__":
    mes = sys.argv[1] if len(sys.argv) > 1 else "2025-12"
    processar_gold(mes)