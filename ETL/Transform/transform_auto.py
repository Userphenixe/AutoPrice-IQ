import re
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
# .../AutoPrice-IQ/ETL
DATA_DIR = BASE_DIR / "data"
PROCESSED_DIR = BASE_DIR / "processed_data"


# =========================
# Helpers génériques
# =========================
def clean_title_series(series: pd.Series) -> pd.Series:
    allowed = re.compile(r'[^a-zA-Z0-9., ]+')
    return (
        series.astype(str)
        .apply(lambda x: allowed.sub("", x))
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def add_marque(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["marque"] = (
        df["title"]
        .astype(str)
        .str.split()
        .apply(lambda mots: " ".join(mots[:3]))
    )
    return df

def to_numeric(df: pd.DataFrame, cols=("price_eur", "year", "kilometers")) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def filter_marque_has_letters(df: pd.DataFrame) -> pd.DataFrame:
    mask = df["marque"].str.contains(r"[A-Za-z]", regex=True, na=False)
    return df.loc[mask].copy().reset_index(drop=True)


def add_host(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["host"] = (
        df["marque"]
        .astype(str)
        .str.split()
        .apply(lambda x: "".join(x[:1]))
    )
    return df


def split_location_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def split_location(loc):
        s = str(loc)
        m = re.search(r"(\d{5})", s)
        if not m:
            return pd.Series([s.strip(), None])
        ville = s[:m.start()].strip()
        cp = m.group(1)
        return pd.Series([ville, cp])

    df[["ville", "code postale"]] = df["location"].apply(split_location)
    return df


def standardize_columns(df: pd.DataFrame, has_location: bool) -> pd.DataFrame:
    df = df.copy()

    if "id" in df.columns:
        df = df.drop(columns=["id"])

    df["title"] = clean_title_series(df["title"])

    df = add_marque(df)

    df = to_numeric(df)

    df = filter_marque_has_letters(df)

    df = add_host(df)

    if has_location:
        df = split_location_column(df)
    else:
        df["location"] = "Unknow"
        df["ville"] = "Unknow"
        df["code postale"] = "Unknow"

    cols_order = [
        "title",
        "marque",
        "host",
        "year",
        "kilometers",
        "price_eur",
        "fuel",
        "gearbox",
        "ville",
        "code postale",
        "location",
    ]
    df = df[cols_order]
    return df

def run_transform(**context):
    """
    Étapes :
      1. Lire les 3 CSV bruts (leboncoin, aramisauto, autoeasy)
      2. Standardiser les colonnes (clean, marque, host, numeric, location)
      3. Concaténer dans un seul DataFrame
      4. Sauvegarder dans processed_data/auto.csv
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df_leboncoin_raw = pd.read_csv(DATA_DIR / "leboncoin.csv")
    df_aramisauto_raw = pd.read_csv(DATA_DIR / "aramisauto.csv")
    df_autoeasy_raw = pd.read_csv(DATA_DIR / "autoeasy.csv")

    df_leboncoin = standardize_columns(df_leboncoin_raw, has_location=True)
    df_aramisauto = standardize_columns(df_aramisauto_raw, has_location=False)
    df_autoeasy = standardize_columns(df_autoeasy_raw, has_location=False)

    df_auto = pd.concat([df_leboncoin, df_aramisauto, df_autoeasy], ignore_index=True)

    dest_file = PROCESSED_DIR / "auto.csv"
    df_auto.to_csv(dest_file, index=False)
    print(f"[TRANSFORM] File saved to: {dest_file}")
