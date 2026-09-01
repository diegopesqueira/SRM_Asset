import sys
from pyspark.sql.functions import col, trim
from receita.spark_session import get_spark_session


def processar_municipios_silver(reference_month: str) -> None:
    spark = get_spark_session("Silver_Municipios")
    input_path = f"s3a://receitas/bronze/municipios/year_month={reference_month}"
    output_path = f"s3a://receitas/silver/municipios/year_month={reference_month}"

    df_bronze = spark.read.parquet(input_path)

    df_silver = (
        df_bronze.withColumn("codigo_municipio", col("codigo").cast("integer"))
        .withColumn("nome_municipio", trim(col("descricao")))
        .dropDuplicates(["codigo_municipio"])
    )

    df_silver.write.mode("overwrite").parquet(output_path)
    print(f"[OK] Silver Municípios gravado com sucesso em: {output_path}")


if __name__ == "__main__":
    mes = sys.argv[1] if len(sys.argv) > 1 else "2025-12"
    processar_municipios_silver(mes)