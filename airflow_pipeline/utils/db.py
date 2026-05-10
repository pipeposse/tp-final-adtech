"""Helper de inserción en Postgres (recos_db) usando psycopg2."""
import os

import psycopg2
from psycopg2.extras import execute_values


def get_db_config() -> dict:
    return dict(
        host=os.environ["RECOS_DB_HOST"],
        port=int(os.getenv("RECOS_DB_PORT", "5432")),
        dbname=os.getenv("RECOS_DB_NAME", "recos_db"),
        user=os.environ["RECOS_DB_USER"],
        password=os.environ["RECOS_DB_PASSWORD"],
    )


def insert_recommendations(df, model: str, ds: str) -> int:
    """
    Inserta filas en recommendations. Idempotente vía ON CONFLICT.
    df debe tener columnas: advertiser_id, product_id, rank, score
    """
    rows = [
        (
            r.advertiser_id,
            model,
            r.product_id,
            int(r.rank),
            float(r.score),
            ds,
        )
        for r in df.itertuples()
    ]
    sql = """
        INSERT INTO recommendations
            (advertiser_id, model, product_id, rank, score, date)
        VALUES %s
        ON CONFLICT (advertiser_id, model, product_id, date)
        DO UPDATE SET rank = EXCLUDED.rank, score = EXCLUDED.score
    """
    with psycopg2.connect(**get_db_config()) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
    return len(rows)
