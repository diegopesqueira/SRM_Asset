import os
import re
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

        prefix_temp = f"extract_{entity}_"
        with tempfile.TemporaryDirectory(prefix=prefix_temp) as temp_dir:
            temp_extract_path = Path(temp_dir)

            print(
                f"[INFO] Extraindo {zip_path.name} para a "
                f"entidade '{entity}'..."
            )
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(temp_extract_path)

            extracted_files = [
                f for f in temp_extract_path.rglob("*") if f.is_file()
            ]

            if not extracted_files:
                print(
                    "[WARN] Nenhum arquivo encontrado dentro de "
                    f"{zip_path.name}"
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
                        f"{new_path.name}, pulando..."
                    )
                else:
                    f.rename(new_path)
                    print(f"[OK] Arquivo gerado: {new_path.name}")

    def run(self) -> None:
        zip_files = sorted(self.zip_directory.glob("*.zip"))
        if not zip_files:
            dir_path = self.zip_directory.resolve()
            print(f"[WARN] Nenhum arquivo ZIP encontrado em {dir_path}")
            return

        print(
            f"[INFO] Iniciando extração de {len(zip_files)} arquivos ZIP..."
        )
        for zip_path in zip_files:
            try:
                self.process_zip(zip_path)
            except Exception as exc:
                print(f"[ERRO] Falha ao processar {zip_path.name}: {exc}")

        print("\n" + "=" * 70)
        print("PROCESSO DE EXTRAÇÃO E PADRONIZAÇÃO FINALIZADO COM SUCESSO")
        print("=" * 70)