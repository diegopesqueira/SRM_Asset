import sys
import tempfile
from pathlib import Path
import pandas as pd
import plotly.express as px
from receita.minio_client import MinIOClient


class GeraDashboard:
    """Gera o gráfico a partir dos dados agregados na Camada Gold."""

    def __init__(
        self,
        reference_month: str,
        minio_client: MinIOClient,
        output_dir: str = "dashboard",
    ) -> None:
        self.reference_month = reference_month
        self.gold_prefix = f"gold/matriz_filial/year_month={reference_month}/"
        self.dashboard_prefix = f"dashboard/year_month={reference_month}"
        self.minio = minio_client
        self.output_dir = Path(output_dir)

    def carregar_dados_gold(self) -> pd.DataFrame:
        objects = list(
            self.minio.client.list_objects(
                self.minio.bucket, prefix=self.gold_prefix, recursive=True
            )
        )

        if not objects:
            print(f"[WARN] Nenhum arquivo encontrado em: {self.gold_prefix}")
            return pd.DataFrame()

        dfs = []
        for obj in objects:
            if obj.object_name.endswith(".parquet"):
                temp_file = Path(tempfile.gettempdir()) / Path(obj.object_name).name
                try:
                    self.minio.client.fget_object(
                        self.minio.bucket, obj.object_name, str(temp_file)
                    )
                    dfs.append(pd.read_parquet(temp_file))
                finally:
                    if temp_file.exists():
                        temp_file.unlink()

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def gerar_e_salvar_grafico(self) -> None:
        df = self.carregar_dados_gold()

        if df.empty:
            print("[WARN] Sem dados na Gold para gerar o gráfico.")
            return

        self.output_dir.mkdir(parents=True, exist_ok=True)
        municipio_nome = df["nome_municipio"].iloc[0] if "nome_municipio" in df.columns else "Geral"

        titulo = f"Distribuição Matrizes vs Filiais - {municipio_nome} ({self.reference_month})"

        fig = px.pie(
            df,
            values="qtd_estabelecimentos",
            names="tipo_estabelecimento",
            title=titulo,
            hole=0.3,
        )
        fig.update_traces(textinfo="label+value+percent", pull=[0.05, 0])
        fig.update_layout(template="plotly_white")

        filename = f"dashboard_{self.reference_month.replace('-', '_')}.png"
        local_filepath = self.output_dir / filename

        try:
            fig.write_image(str(local_filepath), width=1000, height=600, scale=2)
            print(f"[OK] Imagem gerada: {local_filepath}")

            minio_object_name = f"{self.dashboard_prefix}/{filename}"
            self.minio.upload_file(minio_object_name, str(local_filepath))
            print(f"[OK] Dashboard enviado ao MinIO: {minio_object_name}")

        finally:
            if local_filepath.exists():
                local_filepath.unlink()

    def run(self) -> None:
        self.gerar_e_salvar_grafico()


if __name__ == "__main__":
    mes_ref = sys.argv[1] if len(sys.argv) > 1 else "2025-12"
    client = MinIOClient()
    dash = GeraDashboard(reference_month=mes_ref, minio_client=client)
    dash.run()