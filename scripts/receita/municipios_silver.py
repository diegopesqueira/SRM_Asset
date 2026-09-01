import os
import tempfile
from pathlib import Path

import pandas as pd

from receita.helpers import apply_schema
from receita.minio_client import MinIOClient
from receita.schema import MUNICIPIOS_COLUMNS


class CargaMunicipiosSilver:
    """Classe responsável pelo processamento Silver de Municípios."""

    def __init__(
            self, reference_month: str, minio_client: MinIOClient
    ) -> None:
        self.reference_month = reference_month
        self.raw_prefix = "receita/bronze/municipios"
        self.silver_prefix = "receita/silver/municipios"
        self.max_file_size_bytes = 128 * 1024 * 1024  # 128 MB
        self.minio = minio_client

    def estimate_rows_per_file(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 100000

        sample = df.head(min(len(df), 1000))
        temp_file = Path(tempfile.gettempdir()) / "sample_municipios.parquet"
        sample.to_parquet(temp_file, index=False)

        avg_row_size = os.path.getsize(temp_file) / len(sample)

        if temp_file.exists():
            temp_file.unlink()

        return int(self.max_file_size_bytes / avg_row_size)

    def write_parquet(
            self, df: pd.DataFrame, prefix: str, entity: str, part: int
    ) -> None:
        part_name = (
            f"{entity}_silver_{self.reference_month}_part_{part:03d}.parquet"
        )
        temp_file = Path(tempfile.gettempdir()) / part_name

        # Garante uniformidade total de tipos em string antes de gravar o Parquet
        df = df.astype(str)
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

    def run(self) -> None:
        raw_search_prefix = (
            f"{self.raw_prefix}/year_month={self.reference_month}/"
        )
        objects = list(
            self.minio.client.list_objects(
                self.minio.bucket, prefix=raw_search_prefix, recursive=True
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
                df = df.astype(str)
                df = apply_schema(df, MUNICIPIOS_COLUMNS)
                dfs.append(df)
            finally:
                if temp_file.exists():
                    temp_file.unlink()

        if not dfs:
            print(
                f"[WARN] Nenhum arquivo encontrado em {raw_search_prefix}"
            )
            return

        df = pd.concat(dfs, ignore_index=True)

        # Regras de tratamento
        df["codigo"] = (
            pd.to_numeric(df["codigo"], errors="coerce")
            .fillna(-1)
            .astype(int)
        )

        null_values = ["", "nan", "NaN", "None"]
        df["descricao"] = (
            df["descricao"]
            .replace(null_values, pd.NA)
            .fillna("NÃO INFORMADO")
        )

        cols = ["codigo", "descricao"]
        df = df[cols]
        df = df.drop_duplicates()

        # Idempotência: limpa o prefixo Silver antes de gravar
        self.minio.clean_prefix(self.silver_prefix, self.reference_month)

        self.split_and_upload(df, self.silver_prefix, "municipios", 1)

        print("[OK] Silver Municípios concluído com sucesso.")