import os
import tempfile
from pathlib import Path

import pandas as pd

from receita.helpers import apply_schema
from receita.minio_client import MinIOClient
from receita.schema import MUNICIPIOS_COLUMNS


class CargaMunicipiosBronze:
    """Classe responsável pelo processamento Bronze de Municípios."""

    def __init__(
        self,
        input_directory: str,
        reference_month: str,
        minio_client: MinIOClient,
    ) -> None:
        self.input_directory = Path(input_directory)
        self.reference_month = reference_month
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
        self, df: pd.DataFrame, prefix_path: str, entity: str, part: int
    ) -> None:
        part_name = (
            f"{entity}_bronze_{self.reference_month}_part_{part:03d}.parquet"
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
        self.minio.ensure_bucket()

        prefix_path = "bronze/municipios"
        self.minio.clean_prefix(prefix_path, self.reference_month)

        csv_files = sorted(
            [
                f
                for f in self.input_directory.rglob("*.csv")
                if "muni" in f.name.lower()
            ]
        )

        if not csv_files:
            target_path = self.input_directory.resolve()
            print(
                "[WARN] Nenhum arquivo CSV de municípios "
                f"encontrado em: {target_path}"
            )
            return

        part_counter = 1

        for csv_path in csv_files:
            print(f"[INFO] Processando {csv_path.name}...")
            df = pd.read_csv(
                csv_path,
                sep=";",
                header=None,
                names=MUNICIPIOS_COLUMNS,
                dtype=str,
                encoding="iso-8859-1",
            )
            df = apply_schema(df, MUNICIPIOS_COLUMNS)
            part_counter = self.split_and_upload(
                df, prefix_path, "municipios", part_counter
            )

        total_parts = part_counter - 1
        print(
            f"[OK] Processamento concluído. Total de partes geradas: {total_parts}"
        )