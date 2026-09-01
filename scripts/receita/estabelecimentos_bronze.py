import os
import tempfile
from pathlib import Path
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from receita.helpers import apply_schema
from receita.minio_client import MinIOClient
from receita.schema import ESTABELECIMENTOS_COLUMNS


class CargaEstabelecimentoBronze:
    """Classe responsável pelo processamento Bronze de Estabelecimentos."""

    def __init__(
        self,
        input_directory: str,
        reference_month: str,
        minio_client: MinIOClient,
    ) -> None:
        self.base_input_directory = Path(input_directory)
        self.reference_month = reference_month
        # Alvo de 128 MB comprimidos no disco
        self.max_file_size_bytes = 128 * 1024 * 1024
        # Quantidade de linhas por bloco na memória RAM
        self.chunk_size_rows = 100000
        # Cliente MinIO recebido por injeção
        self.minio = minio_client

    def upload_to_minio(
        self, local_path: Path, prefix_path: str, part_name: str
    ) -> None:
        """Envia o arquivo gerado para o MinIO no caminho:

        receita/bronze/estabelecimentos/year_month=YYYY-MM/part_name
        """
        object_name = (
            f"{prefix_path}/year_month={self.reference_month}/{part_name}"
        )
        size_mb = os.path.getsize(local_path) / 1024 / 1024
        print(f"[UPLOAD] {object_name} ({size_mb:.2f} MB reais no disco)")

        self.minio.upload_file(object_name, str(local_path.as_posix()))

        if local_path.exists():
            local_path.unlink()

    def run(self) -> None:
        self.minio.ensure_bucket()

        prefix_path = "receita/bronze/estabelecimentos"
        self.minio.clean_prefix(prefix_path, self.reference_month)

        target_subfolder = f"estabelecimentos_{self.reference_month}"
        full_input_path = (
            Path(self.base_input_directory.as_posix()) / target_subfolder
        )

        csv_files = sorted(full_input_path.glob("*.csv"))
        if not csv_files:
            print(
                "[INFO] Nenhum CSV encontrado em "
                f"{full_input_path.as_posix()} para Estabelecimentos."
            )
            return

        part_counter = 1

        for csv_path in csv_files:
            csv_unix_string = str(csv_path.as_posix())

            print(f"\n[INFO] Lendo {csv_path.name} via PyArrow Streaming...")

            chunks = pd.read_csv(
                csv_unix_string,
                sep=";",
                encoding="iso-8859-1",
                low_memory=False,
                header=None,
                chunksize=self.chunk_size_rows,
                dtype=str,
            )

            writer = None
            current_part_name = (
                f"estabelecimentos_bronze_{self.reference_month}_"
                f"part_{part_counter:03d}.parquet"
            )
            temp_file_path = Path(tempfile.gettempdir()) / current_part_name

            for chunk in chunks:
                # Garante uniformidade total de tipos em cada chunk lido
                chunk = chunk.astype(str)
                chunk = apply_schema(chunk, ESTABELECIMENTOS_COLUMNS)

                table = pa.Table.from_pandas(chunk, preserve_index=False)

                if writer is None:
                    writer = pq.ParquetWriter(
                        str(temp_file_path.as_posix()),
                        table.schema,
                        compression="SNAPPY",
                    )

                writer.write_table(table)

                if os.path.getsize(temp_file_path) >= self.max_file_size_bytes:
                    writer.close()
                    writer = None

                    self.upload_to_minio(
                        temp_file_path, prefix_path, current_part_name
                    )

                    part_counter += 1
                    current_part_name = (
                        f"estabelecimentos_bronze_{self.reference_month}_"
                        f"part_{part_counter:03d}.parquet"
                    )
                    temp_file_path = (
                        Path(tempfile.gettempdir()) / current_part_name
                    )

            if writer is not None:
                writer.close()
                self.upload_to_minio(
                    temp_file_path, prefix_path, current_part_name
                )
                part_counter += 1

        total_parts = part_counter - 1
        print(
            "\n[OK] Processamento concluído. "
            f"Total de partes reais de 128MB geradas: {total_parts}"
        )

if __name__ == "__main__":
    input_dir = (
        sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/dados_temp/unzip"
    )
    mes_ref = sys.argv[2] if len(sys.argv) > 2 else "2025-12"

    # Instancia o cliente do MinIO utilizando as variáveis do ambiente
    minio_client = MinIOClient()

    carga = CargaEstabelecimentoBronze(
        input_directory=input_dir,
        reference_month=mes_ref,
        minio_client=minio_client,
    )
    carga.run()