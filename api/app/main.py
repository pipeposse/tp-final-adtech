"""
AdTech Recommendations API.

Endpoints (alineados con el enunciado):
  GET /                                  -> health-style root
  GET /health                            -> health check
  GET /recommendations/{advertiser_id}/{modelo}
  GET /history/{advertiser_id}/          -> recos de los últimos 7 días
  GET /stats/                            -> estadísticas agregadas

Backend: psycopg2 directo (sin SQLAlchemy).
"""

import ast
import os
from contextlib import asynccontextmanager, contextmanager
from datetime import date, timedelta

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Path

POSTGRES_URI = os.getenv("POSTGRES_URI")
if not POSTGRES_URI:
    raise RuntimeError("Falta la variable de entorno POSTGRES_URI")

# psycopg2 directo no entiende el dialecto SQLAlchemy ("postgresql+psycopg2://").
# Lo normalizamos a la URI estandar libpq antes de pasarla a psycopg2.connect.
if POSTGRES_URI.startswith("postgresql+psycopg2://"):
    POSTGRES_URI = POSTGRES_URI.replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )

MODEL_TABLE_MAP = {
    "TopCTR": "top_ctr",
    "TopProduct": "top_product",
}


@contextmanager
def get_conn():
    conn = psycopg2.connect(POSTGRES_URI)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _parse_products(raw):
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return ast.literal_eval(raw)
        except Exception:
            return [raw]
    return [raw]


def create_logs_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS api_logs (
                    id            SERIAL PRIMARY KEY,
                    advertiser_id TEXT,
                    model         TEXT,
                    date          TEXT,
                    timestamp     TIMESTAMP DEFAULT NOW()
                )
                """
            )


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        create_logs_table()
    except Exception as e:
        print(f"Warning: No se pudo crear tabla de logs: {e}")
    yield


app = FastAPI(title="AdTech Recommendations API", lifespan=lifespan)


@app.get("/")
def root():
    return {"status": "ok", "service": "adtech-recommendations-api"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/recommendations/{advertiser_id}/{modelo}")
def get_recommendations(
    advertiser_id: str = Path(..., description="ID del advertiser"),
    modelo: str = Path(..., description="TopCTR o TopProduct"),
):
    if modelo not in MODEL_TABLE_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Modelo invalido: '{modelo}'. Opciones: {list(MODEL_TABLE_MAP.keys())}",
        )
    table = MODEL_TABLE_MAP[modelo]

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT advertiser_id, top_products, date "
                f"FROM {table} "
                f"WHERE advertiser_id = %s "
                f"ORDER BY date DESC LIMIT 1",
                (advertiser_id,),
            )
            row = cur.fetchone()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"No hay recomendaciones para advertiser '{advertiser_id}' con modelo '{modelo}'.",
        )

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO api_logs (advertiser_id, model, date) VALUES (%s, %s, %s)",
                    (advertiser_id, modelo, str(date.today())),
                )
    except Exception as e:
        print(f"Warning: no se pudo loguear consulta: {e}")

    return {
        "advertiser_id": row["advertiser_id"],
        "model": modelo,
        "date": row["date"],
        "recommendations": _parse_products(row["top_products"]),
    }


@app.get("/history/{advertiser_id}/")
def get_history(advertiser_id: str = Path(...)):
    cutoff = (date.today() - timedelta(days=7)).isoformat()
    history = {"TopCTR": [], "TopProduct": []}

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for modelo, table in MODEL_TABLE_MAP.items():
                cur.execute(
                    f"SELECT advertiser_id, top_products, date "
                    f"FROM {table} "
                    f"WHERE advertiser_id = %s AND date >= %s "
                    f"ORDER BY date DESC",
                    (advertiser_id, cutoff),
                )
                history[modelo] = [
                    {
                        "date": r["date"],
                        "recommendations": _parse_products(r["top_products"]),
                    }
                    for r in cur.fetchall()
                ]

    if not history["TopCTR"] and not history["TopProduct"]:
        raise HTTPException(
            status_code=404,
            detail=f"No hay historial para advertiser '{advertiser_id}'.",
        )

    return {"advertiser_id": advertiser_id, "history": history}


@app.get("/stats/")
def get_stats():
    out = {}

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT date,
                       COUNT(DISTINCT advertiser_id) AS advertisers_consultados,
                       COUNT(*)                      AS consultas_totales
                FROM api_logs
                GROUP BY date
                ORDER BY date DESC
                """
            )
            out["consultas_por_dia"] = [dict(r) for r in cur.fetchall()]

            cur.execute("SELECT COUNT(DISTINCT advertiser_id) AS n FROM top_ctr")
            out["advertisers_top_ctr"] = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(DISTINCT advertiser_id) AS n FROM top_product")
            out["advertisers_top_product"] = cur.fetchone()["n"]

            cur.execute(
                """
                SELECT c.advertiser_id,
                       c.top_products AS ctr_products,
                       p.top_products AS product_products
                FROM top_ctr c
                JOIN top_product p ON c.advertiser_id = p.advertiser_id
                """
            )
            rows = cur.fetchall()

    coincidencias = []
    for r in rows:
        ctr_set = set(_parse_products(r["ctr_products"]))
        prod_set = set(_parse_products(r["product_products"]))
        if not ctr_set or not prod_set:
            continue
        overlap = len(ctr_set & prod_set)
        union = len(ctr_set | prod_set)
        coincidencias.append(
            {
                "advertiser_id": r["advertiser_id"],
                "productos_en_comun": overlap,
                "jaccard": round(overlap / union, 3) if union else 0,
            }
        )
    coincidencias.sort(key=lambda x: x["jaccard"], reverse=True)
    out["coincidencia_entre_modelos"] = coincidencias

    if not out["consultas_por_dia"]:
        out["mensaje_consultas"] = "No hay consultas registradas aun."

    return out
