from pathlib import Path
import sys
from typing import Optional

import requests
from tqdm import tqdm


class Download:
    """Classe responsável pelo download dos dados abertos de CNPJ."""

    def __init__(
        self,
        mes_referencia: str,
        destino: str,
        overwrite: bool = True,
    ) -> None:
        self.mes_referencia = mes_referencia
        self.destino = Path(destino)
        self.destino.mkdir(parents=True, exist_ok=True)
        self.overwrite = overwrite

        url_root = (
            "https://arquivos.receitafederal.gov.br/public.php/dav/files/"
            "gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ"
        )
        self.base_url = f"{url_root}/{mes_referencia}"

        # Monta a lista de arquivos para download
        self.arquivos = [f"Estabelecimentos{i}.zip" for i in range(10)]
        self.arquivos.append("Municipios.zip")

    def baixar_arquivo(self, nome_arquivo: str) -> Optional[Path]:
        url = f"{self.base_url}/{nome_arquivo}"
        caminho = self.destino / nome_arquivo

        if caminho.exists():
            if self.overwrite:
                print(f"[REMOVE] Excluindo arquivo antigo: {nome_arquivo}")
                caminho.unlink()
            else:
                print(f"[SKIP] Arquivo já baixado: {nome_arquivo}")
                return caminho

        print(f"[DOWNLOAD] {nome_arquivo}")
        try:
            resp = requests.get(url, stream=True, timeout=60, verify=False)
            if resp.status_code != 200:
                print(f"[ERRO] {nome_arquivo} (status {resp.status_code})")
                return None

            total = int(resp.headers.get("content-length", 0))

            with open(caminho, "wb") as f:
                with tqdm(
                    desc=nome_arquivo,
                    total=total,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                ) as bar:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                        bar.update(len(chunk))

            print(f"[OK] Arquivo salvo em: {caminho}")
            return caminho
        except Exception as e:
            print(f"[ERRO EXCEÇÃO] Falha ao baixar {nome_arquivo}: {e}")
            return None

    def run(self) -> list[Path]:
        paths = []
        for arq in self.arquivos:
            caminho = self.baixar_arquivo(arq)
            if caminho:
                paths.append(caminho)
        return paths


# Bloco para execução direta por linha de comando (CLI/Airflow)
if __name__ == "__main__":
    mes = sys.argv[1] if len(sys.argv) > 1 else "2025-12"
    pasta_destino = (
        sys.argv[2] if len(sys.argv) > 2 else "/opt/airflow/dados_temp/zip"
    )

    downloader = Download(mes_referencia=mes, destino=pasta_destino)
    downloader.run()