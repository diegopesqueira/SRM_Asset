import os
import tempfile
from pathlib import Path

import pandas as pd

from receita.minio_client import MinIOClient


class CargaMatrizFilialGold:
    """Classe responsável por consolidar a camada Gold de Matriz/Filial."""

    def __init__(
        self, reference_month: str, minio_client: MinIOClient
    ) -> None:
        self.reference_month = reference_month
        self.silver_estab_prefix = "silver/estabelecimentos"
        self.silver_muni_prefix = "silver/municipios"
        self.gold_prefix = "gold/matriz_filial"
        self.max_file_size_bytes = 128 * 1024 * 1024  # 128 MB
        self.minio = minio_client

    def estimate_rows_per_file(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 100000

        sample = df.head(min(len(df), 1000))
        temp_file = Path(tempfile.gettempdir()) / "sample_gold.parquet"
        sample.to_parquet(temp_file, index=False)

        avg_row_size = os.path.getsize(temp_file) / len(sample)

        if temp_file.exists():
            temp_file.unlink()

        return int(self.max_file_size_bytes / avg_row_size)

    def write_parquet(
        self, df: pd.DataFrame, prefix: str, entity: str, part: int
    ) -> None:
        part_name = (
            f"{entity}_gold_{self.reference_month}_part_{part:03d}.parquet"
        )
        temp_file = Path(tempfile.gettempdir()) / part_name
        df.to_parquet(temp_file, index=False)

        object_name = f"{prefix}/year_month={self.reference_month}/{part_name}"
        size_mb = os.path.getsize(temp_file) / 1024 / 1024
        print(f"[UPLOAD] {object_name} ({size_mb:.2f} MB)")

        self.minio.upload_file(object_name, str(temp_file))

        if temp_file.exists():
            temp_file.unlink()

    def split_and_upload(
        self, df: pd.DataFrame, prefix: str, entity: str, start_part: int
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
                self.write_parquet(part_df, prefix, entity, part_counter)
                part_counter += 1

        return part_counter

    def _read_layer(self, prefix: str) -> pd.DataFrame:
        search_path = f"{prefix}/year_month={self.reference_month}/"
        objects = list(
            self.minio.client.list_objects(
                self.minio.bucket, prefix=search_path, recursive=True
            )
        )

        dfs = []
        for obj in objects:
            temp_file = (
                Path(tempfile.gettempdir()) / Path(obj.object_name).name
            )
            try:
                self.minio.client.fget_object(
                    self.minio.bucket, obj.object_name, str(temp_file)
                )
                df = pd.read_parquet(temp_file)
                dfs.append(df)
            finally:
                if temp_file.exists():
                    temp_file.unlink()

        if not dfs:
            print(f"[WARN] Nenhum arquivo encontrado em {search_path}")
            return pd.DataFrame()

        return pd.concat(dfs, ignore_index=True)

    def run(self) -> None:
        print("[INFO] Carregando tabelas da camada Silver...")
        df_estab = self._read_layer(self.silver_estab_prefix)
        df_muni = self._read_layer(self.silver_muni_prefix)

        if df_estab.empty:
            print("[WARN] Abortando processamento Gold: Estabelecimentos vazio.")
            return

        # Enriquecimento de dados (Join Estabelecimentos x Municípios)
        if not df_muni.empty:
            df_gold = df_estab.merge(
                df_muni,
                left_on="municipio",
                right_on="codigo",
                how="left",
                suffixes=("", "_muni"),
            )
            df_gold["nome_municipio"] = df_gold["descricao"].fillna(
                "NÃO INFORMADO"
            )
            df_gold = df_gold.drop(
                columns=["codigo", "descricao"], errors="ignore"
            )
        else:
            df_gold = df_estab.copy()
            df_gold["nome_municipio"] = "NÃO INFORMADO"

        df_gold = df_gold.drop_duplicates()

        # Gravando na camada Gold no MinIO
        self.minio.clean_prefix(self.gold_prefix, self.reference_month)
        self.split_and_upload(df_gold, self.gold_prefix, "matriz_filial", 1)

        print("[OK] Processamento Gold Matriz/Filial concluído com sucesso.")