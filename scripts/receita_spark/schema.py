from pyspark.sql.types import StringType, StructField, StructType

# Schema para leitura bruta do CSV de Estabelecimentos da Receita Federal
SCHEMA_ESTABELECIMENTOS = StructType(
    [
        StructField("cnpj_basico", StringType(), True),
        StructField("cnpj_ordem", StringType(), True),
        StructField("cnpj_dv", StringType(), True),
        StructField("identificador_matriz_filial", StringType(), True),
        StructField("nome_fantasia", StringType(), True),
        StructField("situacao_cadastral", StringType(), True),
        StructField("data_situacao_cadastral", StringType(), True),
        StructField("motivo_situacao_cadastral", StringType(), True),
        StructField("nome_cidade_exterior", StringType(), True),
        StructField("pais", StringType(), True),
        StructField("data_inicio_atividade", StringType(), True),
        StructField("cnae_fiscal_principal", StringType(), True),
        StructField("cnae_fiscal_secundaria", StringType(), True),
        StructField("tipo_logradouro", StringType(), True),
        StructField("logradouro", StringType(), True),
        StructField("numero", StringType(), True),
        StructField("complemento", StringType(), True),
        StructField("bairro", StringType(), True),
        StructField("cep", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("municipio", StringType(), True),
        StructField("ddd_1", StringType(), True),
        StructField("telefone_1", StringType(), True),
        StructField("ddd_2", StringType(), True),
        StructField("telefone_2", StringType(), True),
        StructField("ddd_fax", StringType(), True),
        StructField("fax", StringType(), True),
        StructField("correio_eletronico", StringType(), True),
        StructField("situacao_especial", StringType(), True),
        StructField("data_situacao_especial", StringType(), True),
    ]
)