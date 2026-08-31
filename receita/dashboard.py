import tempfile
from pathlib import Path

import pandas as pd
import plotly.express as px

from receita.minio_client import MinIOClient


class GeraDashboard:
    """Classe responsável por gerar e exportar visualizações do dashboard."""

    def __init__(
        self,
        reference_month: str,
        minio_client: MinIOClient,
        output_dir: str = "dashboard",
    ) -> None:
        self.reference_month = reference_month
        self.gold_prefix = "gold/matriz_filial"
        self.dashboard_prefix = "dashboard"
        self.minio = minio_client
        self.output_dir = Path(output_dir)
        self.resultado = self.carregar_e_preparar_dados()

    def carregar_resultado(self) -> pd.DataFrame:
        prefix = f"{self.gold_prefix}/year_month={self.reference_month}/"
        objects = list(
            self.minio.client.list_objects(
                self.minio.bucket, prefix=prefix, recursive=True
            )
        )

        cols_padrao = [
            "tipo_estabelecimento",
            "nome_municipio",
            "qtd_estabelecimentos",
        ]

        if not objects:
            print(f"[WARN] Nenhum arquivo encontrado no prefixo: {prefix}")
            return pd.DataFrame(columns=cols_padrao)

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
            print("[WARN] Erro ao carregar arquivos Parquet da Gold.")
            return pd.DataFrame(columns=cols_padrao)

        return pd.concat(dfs, ignore_index=True)

    def carregar_e_preparar_dados(self) -> pd.DataFrame:
        """Carrega os dados e garante a agregação necessária para o gráfico."""
        df = self.carregar_resultado()

        if df.empty:
            return df

        # Caso 1: Os dados vieram granulares (não agregados)
        if "identificador_matriz_filial" in df.columns:
            mapeamento = {1: "Matriz", 2: "Filial", "1": "Matriz", "2": "Filial"}
            df["tipo_estabelecimento"] = (
                df["identificador_matriz_filial"]
                .map(mapeamento)
                .fillna("Outros")
            )

            group_cols = ["tipo_estabelecimento"]
            if "nome_municipio" in df.columns:
                group_cols.append("nome_municipio")

            df_agregado = (
                df.groupby(group_cols)
                .size()
                .reset_index(name="qtd_estabelecimentos")
            )
            return df_agregado

        # Caso 2: Os dados já possuem tipo_estabelecimento mas precisam de contagem
        if (
            "tipo_estabelecimento" in df.columns
            and "qtd_estabelecimentos" not in df.columns
        ):
            group_cols = ["tipo_estabelecimento"]
            if "nome_municipio" in df.columns:
                group_cols.append("nome_municipio")

            return (
                df.groupby(group_cols)
                .size()
                .reset_index(name="qtd_estabelecimentos")
            )

        return df

    def gerar_e_salvar_grafico(self) -> None:
        if self.resultado.empty:
            print("[WARN] Não há dados para gerar o gráfico no dashboard.")
            return

        self.output_dir.mkdir(parents=True, exist_ok=True)

        tem_coluna = "nome_municipio" in self.resultado.columns
        municipio_nome = (
            self.resultado["nome_municipio"].iloc[0]
            if tem_coluna and not self.resultado.empty
            else "Geral"
        )

        titulo = (
            f"Distribuição Matrizes vs Filiais - {municipio_nome} "
            f"({self.reference_month})"
        )

        fig = px.pie(
            self.resultado,
            values="qtd_estabelecimentos",
            names="tipo_estabelecimento",
            title=titulo,
            hole=0.3,
        )
        fig.update_traces(textinfo="label+value+percent", pull=[0.05, 0])
        fig.update_layout(template="plotly_white")

        formatted_month = self.reference_month.replace("-", "_")
        filename = f"dashboard_{formatted_month}.png"
        local_filepath = self.output_dir / filename

        try:
            fig.write_image(
                str(local_filepath), width=1000, height=600, scale=2
            )
            print(
                "[OK] Imagem do dashboard gerada localmente: "
                f"{local_filepath}"
            )

            object_folder = (
                f"{self.dashboard_prefix}/year_month={self.reference_month}"
            )
            minio_object_name = f"{object_folder}/{filename}"
            self.minio.upload_file(minio_object_name, str(local_filepath))

            print(
                "[OK] Dashboard enviado com sucesso para o MinIO: "
                f"{minio_object_name}"
            )

        finally:
            if local_filepath.exists():
                local_filepath.unlink()

    def run(self) -> None:
        self.gerar_e_salvar_grafico()