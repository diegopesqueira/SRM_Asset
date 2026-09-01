from pathlib import Path
from typing import Optional

import requests
from tqdm import tqdm


class Download:
    """Classe responsável pelo download dos dados abertos de CNPJ."""

    def __init__(
        self,
        reference_month: str,
        output_dir: str,
        overwrite: bool = True,
    ) -> None:
        self.reference_month = reference_month
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.overwrite = overwrite

        url_root = (
            "https://arquivos.receitafederal.gov.br/public.php/dav/files/"
            "gn672Ad4CF8N6TK/Dados/Cadastros/CNPJ"
        )
        self.base_url = f"{url_root}/{reference_month}"

        # Monta a lista de arquivos para download
        self.arquivos = [f"Estabelecimentos{i}.zip" for i in range(10)]
        self.arquivos.append("Municipios.zip")

    def baixar_arquivo(self, nome_arquivo: str) -> Optional[Path]:
        url = f"{self.base_url}/{nome_arquivo}"
        caminho = self.output_dir / nome_arquivo

        if caminho.exists():
            if self.overwrite:
                print(f"[REMOVE] Excluindo arquivo antigo: {nome_arquivo}")
                caminho.unlink()
            else:
                print(f"[SKIP] Arquivo já baixado: {nome_arquivo}")
                return caminho

        print(f"[DOWNLOAD] {nome_arquivo}")
        resp = requests.get(url, stream=True)
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

    def run(self) -> list[Path]:
        paths = []
        for arq in self.arquivos:
            caminho = self.baixar_arquivo(arq)
            if caminho:
                paths.append(caminho)
        return paths