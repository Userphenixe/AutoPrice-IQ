from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


BASE_DIR = Path(__file__).resolve().parents[1] 
# .../AUTOPRICE-IQ/ETL
DEFAULT_CSV = BASE_DIR / "processed_data" / "auto.csv" 

PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "Ham2603!")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "automotive")

DEFAULT_DB_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)


# ================== HELPERS SQLALCHEMY ==================

def get_engine(db_url: str | None = None) -> Engine:
    """
    Crée un engine SQLAlchemy sur PostgreSQL.

    - db_url : URL complète (optionnel).
      Si None => on utilise DEFAULT_DB_URL construit plus haut.
    """
    url = db_url or DEFAULT_DB_URL
    return create_engine(url)


# ================== FONCTIONS DE LOAD ==================

def load_dataframe_to_table(
    df: pd.DataFrame,
    table_name: str = "ads",
    schema: str | None = "public",
    if_exists: str = "append",
    chunksize: int = 1_000,
    db_url: str | None = None,
    engine: Engine | None = None,
) -> None:
    """
    Charge un DataFrame dans une table PostgreSQL en utilisant pandas + SQLAlchemy.

    - df : DataFrame déjà préparé (mêmes colonnes que la table cible, ici 'ads'
           sans la colonne id qui est générée côté base).
    - table_name : nom de la table cible (par défaut 'ads').
    - schema : schéma ('public' par défaut).
    - if_exists : 'append' (par défaut), 'replace', 'fail'...
    - chunksize : nombre de lignes par batch.
    - db_url / engine : soit tu passes un engine, soit une db_url, soit rien
      (et on utilise la config par défaut).
    """
    if engine is None:
        engine = get_engine(db_url)

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=chunksize,
    )

    print(f"{len(df)} lignes insérées dans {schema}.{table_name}")


def load_csv_to_ads(
    csv_path: str | Path | None = None,
    db_url: str | None = None,
    truncate_first: bool = False,
) -> None:
    """
    Charge le CSV (auto.csv) dans la table public.ads.

    - csv_path : chemin vers le CSV final (auto.csv).
                 Si None => processed_data/auto.csv
    - db_url : URL PostgreSQL (optionnelle).
    - truncate_first : si True, fait un TRUNCATE TABLE avant d'insérer.
    """
    csv_path = Path(csv_path or DEFAULT_CSV)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV introuvable : {csv_path}")

    df = pd.read_csv(csv_path)

    engine = get_engine(db_url)

    if truncate_first:
        with engine.begin() as conn:
            conn.execute("TRUNCATE TABLE public.ads RESTART IDENTITY;")
        print("Table public.ads tronquée (TRUNCATE + RESTART IDENTITY).")

    load_dataframe_to_table(
        df=df,
        table_name="ads",
        schema="public",
        if_exists="append",
        engine=engine,
    )

if __name__ == "__main__":
    load_csv_to_ads()
