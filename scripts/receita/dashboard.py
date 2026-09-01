import sys
import tempfile
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from receita.minio_client import MinIOClient


class GeraDashboard:
    """Classe responsável por gerar e exportar o dashboard com legenda detalhada via Matplotlib."""

    def __init__(
            self,
            reference_month: str,
            minio_client: MinIOClient,
            output_dir: str = "dashboard",
    ) -> None:
        self.reference_month = reference_month
        self.gold_prefix = "receita/gold/matriz_filial"
        self.dashboard_prefix = "receita/dashboard"
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
                df = df.astype(str)
                dfs.append(df)
            finally:
                if temp_file.exists():
                    temp_file.unlink()

        if not dfs:
            print("[WARN] Erro ao carregar arquivos Parquet da Gold.")
            return pd.DataFrame(columns=cols_padrao)

        return pd.concat(dfs, ignore_index=True)

    def carregar_e_preparar_dados(self) -> pd.DataFrame:
        df = self.carregar_resultado()

        if df.empty:
            return df

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
            df_agregado["qtd_estabelecimentos"] = pd.to_numeric(df_agregado["qtd_estabelecimentos"])
            return df_agregado

        return df

    def gerar_e_salvar_grafico(self) -> None:
        if self.resultado.empty:
            raise ValueError(f"[ERRO] Não há dados para gerar o gráfico no mês {self.reference_month}.")

        self.output_dir.mkdir(parents=True, exist_ok=True)

        df_plot = self.resultado.groupby("tipo_estabelecimento")["qtd_estabelecimentos"].sum().reset_index()

        tem_coluna = "nome_municipio" in self.resultado.columns
        municipio_nome = (
            self.resultado["nome_municipio"].iloc[0]
            if tem_coluna and not self.resultado.empty
            else "São Paulo"
        )

        titulo = (
            f"Distribuição Matrizes vs Filiais - {municipio_nome} "
            f"({self.reference_month})"
        )

        # Cores para cada categoria
        cores = ["#2b5c8f", "#d95f02"]

        plt.figure(figsize=(10, 6))
        wedges, texts, autotexts = plt.pie(
            df_plot["qtd_estabelecimentos"],
            labels=None,  # Remove labels direto no gráfico para evitar poluição
            autopct="%1.1f%%",
            startangle=140,
            colors=cores,
            pctdistance=0.75
        )

        plt.title(titulo, fontsize=12, fontweight="bold", pad=20)

        # Criação dos patches (quadradinhos) com o nome e a quantidade exata para a legenda
        patches = []
        for i, row in df_plot.iterrows():
            qtd_formatada = f"{int(row['qtd_estabelecimentos']):,}".replace(",", ".")
            label_legenda = f"{row['tipo_estabelecimento']}: {qtd_formatada}"
            patches.append(mpatches.Patch(color=cores[i % len(cores)], label=label_legenda))

        # Posiciona a legenda no canto inferior esquerdo
        plt.legend(
            handles=patches,
            loc="lower left",
            bbox_to_anchor=(0.0, 0.0),
            fontsize=10,
            frameon=True
        )

        plt.tight_layout()

        formatted_month = self.reference_month.replace("-", "_")
        filename = f"dashboard_{formatted_month}.png"
        local_filepath = self.output_dir / filename

        try:
            plt.savefig(str(local_filepath), dpi=300)
            plt.close()
            print(f"[OK] Imagem gerada com legenda: {local_filepath}")

            object_folder = (
                f"{self.dashboard_prefix}/year_month={self.reference_month}"
            )
            minio_object_name = f"{object_folder}/{filename}"
            self.minio.upload_file(minio_object_name, str(local_filepath))

            print(f"[OK] Dashboard enviado para o MinIO: {minio_object_name}")

        finally:
            if local_filepath.exists():
                local_filepath.unlink()

    def run(self) -> None:
        self.gerar_e_salvar_grafico()


if __name__ == "__main__":
    ref_month = sys.argv[1] if len(sys.argv) > 1 else "2026-03"
    minio_client = MinIOClient(bucket_name="receita")

    dashboard = GeraDashboard(
        reference_month=ref_month,
        minio_client=minio_client,
    )
    dashboard.run()