import re
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path


class UnzipFiles:
    """Classe responsável por descompactar e padronizar arquivos ZIP."""

    def __init__(
        self, zip_directory: str, reference_month: str, output_directory: str
    ) -> None:
        self.zip_directory = Path(zip_directory)
        self.reference_month = reference_month
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(parents=True, exist_ok=True)
        self.entity_part_counters: dict[str, int] = {}

    def _normalize_entity_name(self, raw_name: str) -> str:
        """Remove números ao final do nome e mapeia siglas da Receita Federal

        para a nomenclatura padrão utilizada nas etapas de carga.
        """
        clean_name = re.sub(r"\d+$", "", raw_name).lower()

        entity_map = {
            "estab": "Estabelecimentos",
            "estabelec": "Estabelecimentos",
            "estabelecimentos": "Estabelecimentos",
            "municsv": "Municipios",
            "muni": "Municipios",
            "municipios": "Municipios",
            "empcsv": "Empresas",
            "empren": "Empresas",
            "empresas": "Empresas",
            "sociocsv": "Socios",
            "socio": "Socios",
            "socios": "Socios",
            "simples": "Simples",
            "cnae": "Cnaes",
            "cnaes": "Cnaes",
            "natju": "Naturezas",
            "quals": "Qualificacoes",
            "pais": "Paises",
        }

        return entity_map.get(clean_name, clean_name.capitalize())

    def process_zip(self, zip_path: Path) -> None:
        entity = self._normalize_entity_name(zip_path.stem)
        target_folder = f"{entity}_{self.reference_month}"
        final_target_dir = self.output_directory / target_folder
        final_target_dir.mkdir(parents=True, exist_ok=True)

        prefix_temp = f"_tmp_extract_{entity}_"
        # dir=final_target_dir garante que a pasta temporária seja criada no mesmo volume de destino
        with tempfile.TemporaryDirectory(
            prefix=prefix_temp, dir=final_target_dir
        ) as temp_dir:
            temp_extract_path = Path(temp_dir)

            print(
                f"[INFO] Extraindo {zip_path.name} para a "
                f"entidade '{entity}'...",
                flush=True,
            )
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(temp_extract_path)

            extracted_files = [
                f for f in temp_extract_path.rglob("*") if f.is_file()
            ]

            if not extracted_files:
                print(
                    "[WARN] Nenhum arquivo encontrado dentro de "
                    f"{zip_path.name}",
                    flush=True,
                )
                return

            for f in sorted(extracted_files):
                current_part = self.entity_part_counters.get(entity, 1)
                self.entity_part_counters[entity] = current_part + 1

                new_name = (
                    f"{entity.lower()}_{self.reference_month}_"
                    f"part_{current_part:03d}.csv"
                )
                new_path = final_target_dir / new_name

                if new_path.exists():
                    print(
                        "[INFO] Arquivo já existe no destino: "
                        f"{new_path.name}, pulando...",
                        flush=True,
                    )
                else:
                    shutil.move(str(f), str(new_path))
                    print(
                        f"[OK] Arquivo gerado: {new_path.name}",
                        flush=True,
                    )

    def run(self) -> None:
        zip_files = sorted(self.zip_directory.glob("*.zip"))
        if not zip_files:
            dir_path = self.zip_directory.resolve()
            print(
                f"[WARN] Nenhum arquivo ZIP encontrado em {dir_path}",
                flush=True,
            )
            return

        print(
            f"[INFO] Iniciando extração de {len(zip_files)} arquivos ZIP...",
            flush=True,
        )
        for zip_path in zip_files:
            try:
                self.process_zip(zip_path)
            except Exception as exc:
                print(
                    f"[ERRO] Falha ao processar {zip_path.name}: {exc}",
                    flush=True,
                )

        print("\n" + "=" * 70, flush=True)
        print(
            "PROCESSO DE EXTRAÇÃO E PADRONIZAÇÃO FINALIZADO COM SUCESSO",
            flush=True,
        )
        print("=" * 70, flush=True)


if __name__ == "__main__":
    zip_dir = (
        sys.argv[1] if len(sys.argv) > 1 else "/opt/airflow/dados_temp/zip"
    )
    mes_ref = sys.argv[2] if len(sys.argv) > 2 else "2025-12"
    out_dir = (
        sys.argv[3] if len(sys.argv) > 3 else "/opt/airflow/dados_temp/unzip"
    )

    unzipper = UnzipFiles(
        zip_directory=zip_dir, reference_month=mes_ref, output_directory=out_dir
    )
    unzipper.run()