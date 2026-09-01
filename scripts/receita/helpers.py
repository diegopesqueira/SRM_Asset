import unicodedata
import pandas as pd


def normalize_column(name: str) -> str:

    name_str = str(name)
    nfkd = unicodedata.normalize("NFKD", name_str)
    no_accent = "".join([c for c in nfkd if not unicodedata.combining(c)])
    clean = no_accent.replace(" ", "_").replace("/", "_")
    return clean.lower()


def apply_schema(df: pd.DataFrame, expected_columns: list) -> pd.DataFrame:

    normalized_expected = [normalize_column(c) for c in expected_columns]
    df.columns = normalized_expected[:len(df.columns)]

    for col in normalized_expected:
        if col not in df.columns:
            df[col] = ""

    return df[normalized_expected]
