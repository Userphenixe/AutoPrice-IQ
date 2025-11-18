from pathlib import Path
import re
import pandas as pd

# ---------- Config chemins ----------

BASE_DIR = Path(__file__).resolve().parents[1]          # .../AUTOPRICE-IQ/ETL
DATA_DIR = BASE_DIR / "data"
PROCESSED_DIR = BASE_DIR / "processed_data"
DEST_FILE = PROCESSED_DIR / "auto.csv"

# ---------- Regex / helpers communs ----------

ALLOWED_CHARS_RE = re.compile(r"[^a-zA-Z0-9., ]+")

def clean_title(series: pd.Series) -> pd.Series:
    """Garde uniquement lettres/chiffres/points/virgules + normalise les espaces."""
    return (
        series.astype(str)
        .apply(lambda x: ALLOWED_CHARS_RE.sub("", x))
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )

def add_marque(series: pd.Series) -> pd.Series:
    """Crée la marque : 3 premiers mots du titre."""
    return (
        series.astype(str)
        .str.split()
        .apply(lambda mots: " ".join(mots[:3]))
    )

def cast_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Convertit en numérique avec errors='coerce'."""
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def add_host(df: pd.DataFrame) -> pd.DataFrame:
    """Host = premier mot de la marque (ex: Renault, BMW…)."""
    df["host"] = (
        df["marque"]
        .astype(str)
        .str.split()
        .apply(lambda x: "".join(x[:1]))
    )
    return df

def filter_marque_with_letters(df: pd.DataFrame) -> pd.DataFrame:
    """Supprime les lignes où la marque ne contient pas de lettres (ex: '307')."""
    mask = df["marque"].str.contains(r"[A-Za-z]", regex=True, na=False)
    return df[mask].copy().reset_index(drop=True)

# ---------- Spécifique Leboncoin ----------

def split_location(loc: str):
    """
    Sépare location en (ville, code postale).
    Ex: 'Metz 57000' -> ('Metz', '57000')
    """
    s = str(loc)
    m = re.search(r"(\d{5})", s)
    if not m:
        return pd.Series([s.strip(), None])
    ville = s[:m.start()].strip()
    cp = m.group(1)
    return pd.Series([ville, cp])

def transform_leboncoin(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    # on retire la colonne id venant du scraping
    if "id" in df.columns:
        df = df.drop(columns=["id"])

    # nettoyage du titre + marque
    df["title"] = clean_title(df["title"])
    df["marque"] = add_marque(df["title"])

    # numeriques
    df = cast_numeric(df, ["price_eur", "year", "kilometers"])

    # split ville / code postale à partir de location
    df[["ville", "code postale"]] = df["location"].apply(split_location)

    # filtre marques sans lettres & host
    df = filter_marque_with_letters(df)
    df = add_host(df)

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
    return df[cols_order]


# ---------- Spécifique Aramisauto ----------

def transform_aramisauto(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    # colonnes inutiles dans ton notebook
    for col in ["condition", "id", "currency"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    df = df.dropna()

    df["title"] = clean_title(df["title"])
    df["marque"] = add_marque(df["title"])

    df = cast_numeric(df, ["price_eur", "year", "kilometers"])
    # si certaines années sont NaN, tu peux les drop :
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    df = filter_marque_with_letters(df)
    df = add_host(df)

    # infos de localisation inexistantes → 'Unknow' comme dans ton code
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
    return df[cols_order]


# ---------- Spécifique Autoeasy ----------

def transform_autoeasy(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    if "id" in df.columns:
        df = df.drop(columns=["id"])

    df = df.dropna()

    df["title"] = clean_title(df["title"])
    df["marque"] = add_marque(df["title"])

    df = cast_numeric(df, ["price_eur", "year", "kilometers"])

    df = filter_marque_with_letters(df)
    df = add_host(df)

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
    return df[cols_order]


# ---------- Pipeline complet ----------

def build_auto_dataset(
    leboncoin_path: Path | None = None,
    aramis_path: Path | None = None,
    autoeasy_path: Path | None = None,
    dest_file: Path | None = None,
) -> Path:
    """Construit le dataset final et l'enregistre dans processed_data/auto.csv."""
    leboncoin_path = leboncoin_path or (DATA_DIR / "leboncoin.csv")
    aramis_path = aramis_path or (DATA_DIR / "aramisauto.csv")
    autoeasy_path = autoeasy_path or (DATA_DIR / "autoeasy.csv")
    dest = dest_file or DEST_FILE

    df_leboncoin = transform_leboncoin(leboncoin_path)
    df_aramis = transform_aramisauto(aramis_path)
    df_autoeasy = transform_autoeasy(autoeasy_path)

    df_auto = pd.concat([df_leboncoin, df_aramis, df_autoeasy], ignore_index=True)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df_auto.to_csv(dest, index=False)
    print(f"File downloaded successfully ! ({df_auto.shape[0]} lignes) -> {dest}")
    return dest


if __name__ == "__main__":
    build_auto_dataset()
