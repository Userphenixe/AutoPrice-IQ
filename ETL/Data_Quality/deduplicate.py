from __future__ import annotations

import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ================== CONFIG DB (identique à load_to_db) ==================

PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "Ham2603!")
PG_HOST = os.getenv("PG_HOST", "host.docker.internal")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "automotive")

DEFAULT_DB_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

print(
    f"[DEDUP] Using DB URL: postgresql+psycopg2://{PG_USER}:***@{PG_HOST}:{PG_PORT}/{PG_DB}"
)


# ================== HELPER ENGINE ==================

def get_engine(db_url: str | None = None) -> Engine:
    """
    Retourne un engine SQLAlchemy basé sur PostgreSQL.
    """
    return create_engine(db_url or DEFAULT_DB_URL)


# ================== FONCTION : CALL PROCEDURE ==================

def call_remove_duplicates(engine: Engine | None = None, db_url: str | None = None) -> None:
    """
    Appelle la procédure PostgreSQL remove_ads_duplicates()
    pour supprimer les doublons dans la table public.ads.

    Utilisable indépendamment ou depuis un DAG Airflow.
    """

    engine = engine or get_engine(db_url)

    print("[DEDUP] Starting duplicate removal...")

    try:
        with engine.begin() as conn:
            conn.execute(text("CALL remove_ads_duplicates();"))

        print("Procedure remove_ads_duplicates() executed successfully.")

    except Exception as e:
        print(f"ERROR while executing stored procedure: {e}")
        raise


# ================== MAIN (exécution directe) ==================

if __name__ == "__main__":
    call_remove_duplicates()
