import os
import tempfile
from pathlib import Path

import pandas as pd

from helpers import apply_schema
from minio_client import MinIOClient
from schema import ESTABELECIMENTOS_COLUMNS


class CargaEstabelecimentosSilver:
    """Classe responsável pelo processamento da camada Silver de Estabelecimentos."""

    def __init__(self, reference_month: str, minio_client: MinIOClient):
        self.reference_month = reference_month
        self.raw_prefix = "bronze/estabelecimentos"
        self.silver_prefix = "silver/estabelecimentos"
        self.max_file_size_bytes = 128 * 1024 * 1024  # 128 MB
        self.minio = minio_client

    def estimate_rows_per_file(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 100000

        sample = df.head(min(len(df), 1000))
        temp_file = Path(tempfile.gettempdir()) / "sample_estab_silver.parquet"
        sample.to_parquet(temp_file, index=False)

        avg_row_size = os.path.getsize(temp_file) / len(sample)

        if temp_file.exists():
            temp_file.unlink()

        return int(self.max_file_size_bytes / avg_row_size)

    def write_parquet(
        self, df: pd.DataFrame, prefix_path: str, entity: str, part: int
    ) -> None:
        part_name = (
            f"{entity}_silver_{self.reference_month}_part_{part:03d}.parquet"
        )
        temp_file = Path(tempfile.gettempdir()) / part_name
        df.to_parquet(temp_file, index=False)

        object_name = (
            f"{prefix_path}/year_month={self.reference_month}/{part_name}"
        )
        size_mb = os.path.getsize(temp_file) / 1024 / 1024
        print(f"[UPLOAD] {object_name} ({size_mb:.2f} MB)")

        self.minio.upload_file(object_name, str(temp_file))

        if temp_file.exists():
            temp_file.unlink()

    def split_and_upload(
        self, df: pd.DataFrame, prefix_path: str, entity: str, start_part: int
    ) -> int:
        rows = len(df)
        if rows == 0:
            return start_part

        rows_per_file = self.estimate_rows_per_file(df)
        num_parts = (rows // rows_per_file) + (
            1 if rows % rows_per_file != 0 else 0
        )
        part_counter = start_part

        for i in range(num_parts):
            start = i * rows_per_file
            end = min((i + 1) * rows_per_file, rows)
            part_df = df.iloc[start:end]
            if not part_df.empty:
                self.write_parquet(part_df, prefix_path, entity, part_counter)
                part_counter += 1

        return part_counter

    def run(self) -> None:
        prefix_search = f"{self.raw_prefix}/year_month={self.reference_month}/"
        objects = list(
            self.minio.client.list_objects(
                self.minio.bucket, prefix=prefix_search, recursive=True
            )
        )

        if not objects:
            print(f"[WARN] Nenhum arquivo encontrado em {prefix_search}")
            return

        print(
            f"[INFO] Processando {len(objects)} arquivos Parquet em modo Chunk..."
        )
        filtered_dfs = []

        for obj in objects:
            temp_file = (
                Path(tempfile.gettempdir()) / Path(obj.object_name).name
            )

            try:
                self.minio.client.fget_object(
                    self.minio.bucket, obj.object_name, str(temp_file)
                )
                df_chunk = pd.read_parquet(temp_file)

                situacao = pd.to_numeric(
                    df_chunk["situacao_cadastral"], errors="coerce"
                )
                muni = pd.to_numeric(df_chunk["municipio"], errors="coerce")

                df_chunk = df_chunk[(situacao == 2) & (muni == 7107)].copy()

                if not df_chunk.empty:
                    filtered_dfs.append(df_chunk)

            finally:
                if temp_file.exists():
                    temp_file.unlink()

        if not filtered_dfs:
            print(
                "[WARN] Nenhum registro encontrado para São Paulo (7107) e Ativos (2)."
            )
            return

        df = pd.concat(filtered_dfs, ignore_index=True)
        print(f"[INFO] Registros filtrados acumulados: {len(df)}")

        df = df.astype(str)
        df = apply_schema(df, ESTABELECIMENTOS_COLUMNS)

        # Regras de transformação e limpeza
        df["numero_cnpj"] = (
            df["cnpj_basico"].fillna("").astype(str).str.zfill(8)
            + df["cnpj_ordem"].fillna("").astype(str).str.zfill(4)
            + df["cnpj_dv"].fillna("").astype(str).str.zfill(2)
        )
        cols = ["numero_cnpj"] + [c for c in df.columns if c != "numero_cnpj"]
        df = df[cols]

        df["identificador_matriz_filial"] = (
            pd.to_numeric(df["identificador_matriz_filial"], errors="coerce")
            .fillna(-1)
            .astype(int)
        )

        null_values = ["", "nan", "NaN", "None"]

        df["nome_fantasia"] = (
            df["nome_fantasia"]
            .replace(null_values, pd.NA)
            .fillna("NÃO INFORMADO")
        )
        df["situacao_cadastral"] = (
            pd.to_numeric(df["situacao_cadastral"], errors="coerce")
            .fillna(-1)
            .astype(int)
        )
        df["data_situacao_cadastral"] = (
            df["data_situacao_cadastral"]
            .replace(null_values, pd.NA)
            .fillna("19000101")
        )
        df["motivo_situacao_cadastral"] = (
            pd.to_numeric(df["motivo_situacao_cadastral"], errors="coerce")
            .fillna(-1)
            .astype(int)
        )
        df["nome_da_cidade_no_exterior"] = (
            df["nome_da_cidade_no_exterior"]
            .replace(null_values, pd.NA)
            .fillna("NÃO INFORMADO")
        )
        df["pais"] = (
            pd.to_numeric(df["pais"], errors="coerce").fillna(-1).astype(int)
        )
        df["data_de_inicio_atividade"] = (
            df["data_de_inicio_atividade"]
            .replace(null_values, pd.NA)
            .fillna("19000101")
        )
        df["cnae_fiscal_principal"] = (
            pd.to_numeric(df["cnae_fiscal_principal"], errors="coerce")
            .fillna(-1)
            .astype(int)
        )
        df["cnae_fiscal_secundaria"] = (
            df["cnae_fiscal_secundaria"]
            .replace(null_values, pd.NA)
            .fillna("NÃO INFORMADO")
        )
        df["tipo_de_logradouro"] = (
            df["tipo_de_logradouro"]
            .replace(null_values, pd.NA)
            .fillna("NÃO INFORMADO")
        )
        df["logradouro"] = (
            df["logradouro"]
            .replace(null_values, pd.NA)
            .fillna("NÃO INFORMADO")
        )
        df["numero"] = (
            df["numero"].replace(null_values, pd.NA).fillna("S/N")
        )
        df["complemento"] = (
            df["complemento"]
            .replace(null_values, pd.NA)
            .fillna("SEM COMPLEMENTO")
        )
        df["bairro"] = (
            df["bairro"]
            .replace(null_values, pd.NA)
            .fillna("NÃO INFORMADO")
        )
        df["cep"] = (
            df["cep"].replace(null_values, pd.NA).fillna("00000000")
        )
        df["uf"] = df["uf"].replace(null_values, pd.NA).fillna("NI")
        df["municipio"] = (
            pd.to_numeric(df["municipio"], errors="coerce")
            .fillna(-1)
            .astype(int)
        )
        df["ddd_1"] = (
            pd.to_numeric(df["ddd_1"], errors="coerce").fillna(-1).astype(int)
        )
        df["telefone_1"] = (
            pd.to_numeric(df["telefone_1"], errors="coerce")
            .fillna(-1)
            .astype(int)
        )
        df["ddd_2"] = (
            pd.to_numeric(df["ddd_2"], errors="coerce").fillna(-1).astype(int)
        )
        df["telefone_2"] = (
            pd.to_numeric(df["telefone_2"], errors="coerce")
            .fillna(-1)
            .astype(int)
        )
        df["ddd_do_fax"] = (
            pd.to_numeric(df["ddd_do_fax"], errors="coerce")
            .fillna(-1)
            .astype(int)
        )
        df["fax"] = (
            pd.to_numeric(df["fax"], errors="coerce").fillna(-1).astype(int)
        )
        df["correio_eletronico"] = (
            df["correio_eletronico"]
            .replace(null_values, pd.NA)
            .fillna("NÃO INFORMADO")
        )
        df["situacao_especial"] = (
            df["situacao_especial"]
            .replace(null_values, pd.NA)
            .fillna("NÃO INFORMADO")
        )
        df["data_da_situacao_especial"] = (
            df["data_da_situacao_especial"]
            .replace(null_values, pd.NA)
            .fillna("19000101")
        )

        df = df.drop(
            columns=["cnpj_basico", "cnpj_ordem", "cnpj_dv"], errors="ignore"
        )
        df = df.drop_duplicates()

        # Salvamento de forma idempotente
        self.minio.clean_prefix(self.silver_prefix, self.reference_month)
        self.split_and_upload(df, self.silver_prefix, "estabelecimentos", 1)

        print("[OK] Processamento Silver Estabelecimentos concluído com sucesso!")