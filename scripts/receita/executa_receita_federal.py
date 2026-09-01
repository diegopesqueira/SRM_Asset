import os
from pathlib import Path

from dashboard import GeraDashboard
from descompacta_arquivo import UnzipFiles
from download_arquivos import Download
from estabelecimentos_silver import CargaEstabelecimentosSilver
from estabelecimentos_bronze import CargaEstabelecimentoBronze
from matriz_filial_gold import CargaMatrizFilialGold
from minio_client import MinIOClient
from municipios_silver import CargaMunicipiosSilver
from municipios_bronze import CargaMunicipiosBronze


class ExecutaReceitaFederal:

    def __init__(
        self,
        mes_referencia: str,
        base_dir: str = "/data",
        bucket_name: str = "receita",
    ):
        self.mes_referencia = mes_referencia
        self.base_dir = Path(base_dir) / mes_referencia
        self.bucket_name = bucket_name

        # Estrutura de pastas padronizada para o container
        self.zips_dir = str(self.base_dir / "zips")
        self.unzip_dir = str(self.base_dir / "unzip_files")

        # Instancia o cliente MinIO
        self.minio = MinIOClient(bucket_name=self.bucket_name)
        self.minio.ensure_bucket()

    def run(self):
        print("=" * 70)
        print(
            f"[INFO] Iniciando pipeline Receita Federal - {self.mes_referencia}"
        )
        print("=" * 70)

       # 1. Bronze - Download dos arquivos .zip
        print("\n[ETAPA 1] Realizando Download dos arquivos...")
        downloader = Download(
            mes_referencia=self.mes_referencia, destino=self.zips_dir
        )
        downloader.run()

        # 2. Bronze - Descompactação dos arquivos .zip para unzip_files/
        print("\n[ETAPA 2] Extraindo arquivos .zip...")
        unzipper = UnzipFiles(
            zip_directory=self.zips_dir,
            reference_month=self.mes_referencia,
            output_directory=self.unzip_dir,
        )
        unzipper.run()

        # 3. Silver - Carga Estabelecimentos Raw (Lê da pasta unzip_files/)
        print("\n[ETAPA 3] Processando Estabelecimentos Bronze...")
        carga_est_raw = CargaEstabelecimentoBronze(
            input_directory=self.unzip_dir,
            reference_month=self.mes_referencia,
            minio_client=self.minio,
        )
        carga_est_raw.run()

        # 4. Silver - Carga Municípios Raw (Lê da pasta unzip_files/)
        print("\n[ETAPA 4] Processando Municípios Bronze...")
        carga_mun_raw = CargaMunicipiosBronze(
            input_directory=self.unzip_dir,
            reference_month=self.mes_referencia,
            minio_client=self.minio,
        )
        carga_mun_raw.run()

        # 5. Silver - Curated Estabelecimentos
        print("\n[ETAPA 5] Gerando camada Silver de Estabelecimentos...")
        carga_est_cur = CargaEstabelecimentosSilver(
            reference_month=self.mes_referencia, minio_client=self.minio
        )
        carga_est_cur.run()

        # 6. Silver - Curated Municípios
        print("\n[ETAPA 6] Gerando camada Silver de Municípios...")
        carga_mun_cur = CargaMunicipiosSilver(
            reference_month=self.mes_referencia, minio_client=self.minio
        )
        carga_mun_cur.run()

        # 7. Gold - Modelo Matrizes vs Filiais
        print("\n[ETAPA 7] Gerando Modelo Gold (Matriz vs Filial)...")
        carga_gold = CargaMatrizFilialGold(
            reference_month=self.mes_referencia, minio_client=self.minio
        )
        carga_gold.run()

        # 8. Dashboard
        print("\n[ETAPA 8] Gerando dados do Dashboard...")
        dashboard = GeraDashboard(
            reference_month=self.mes_referencia, minio_client=self.minio
        )
        dashboard.run()

        print("=" * 70)
        print("[INFO] Pipeline concluída com sucesso!")
        print("=" * 70)


if __name__ == "__main__":
    # Permite passar a pasta base via variável de ambiente ou usar o padrão '/data' do container
    BASE_DIR = os.getenv("DATA_DIR", "/data")

    pipeline = ExecutaReceitaFederal(
        mes_referencia="2026-03", base_dir=BASE_DIR, bucket_name="receita"
    )
    pipeline.run()