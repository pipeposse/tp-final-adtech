"""
AdTech Recommendations API.
Endpoints (alineados con el enunciado):
  GET /                                  -> health-style root
  GET /health                            -> health check
  GET /recommendations/{advertiser_id}/{modelo}
  GET /history/{advertiser_id}/          -> recos de los últimos 7 días
  GET /stats/                            -> estadísticas agregadas

Backend: psycopg2 directo. Lee de la tabla normalizada `recommendations`
(una fila por (advertiser_id, model, product_id, date)). La tabla
`api_logs` la sigue creando esta API en el lifespan startup.
"""
import os
from collections import defaultdict
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

# Mapping del nombre PascalCase que llega por path al valor almacenado en
# la columna `recommendations.model` (snake_case lowercase).
MODEL_DB_MAP = {
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
    if modelo not in MODEL_DB_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Modelo invalido: '{modelo}'. Opciones: {list(MODEL_DB_MAP.keys())}",
        )
    db_model = MODEL_DB_MAP[modelo]

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # "Dame los 20 productos mas recientes para ese advertiser+modelo,
            # ordenados por fecha DESC y luego por rank."
            cur.execute(
                """
                SELECT product_id, date
                FROM recommendations
                WHERE advertiser_id = %s AND model = %s
                ORDER BY date DESC, rank
                LIMIT 20
                """,
                (advertiser_id, db_model),
            )
            rows = cur.fetchall()

    if not rows:
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
        "advertiser_id": advertiser_id,
        "model": modelo,
        "date": rows[0]["date"].isoformat(),
        "recommendations": [r["product_id"] for r in rows],
    }


@app.get("/history/{advertiser_id}/")
def get_history(advertiser_id: str = Path(...)):
    cutoff = date.today() - timedelta(days=7)
    history = {"TopCTR": [], "TopProduct": []}

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            for modelo, db_model in MODEL_DB_MAP.items():
                # "Dame todos los productos de ese advertiser+modelo desde
                # el cutoff (hoy - 7 dias) hasta hoy, ordenados."
                cur.execute(
                    """
                    SELECT date, product_id
                    FROM recommendations
                    WHERE advertiser_id = %s
                      AND model = %s
                      AND date >= %s
                    ORDER BY date DESC, rank
                    """,
                    (advertiser_id, db_model, cutoff),
                )
                # Agrupar las filas por fecha (una fila por producto).
                grouped = {}
                for r in cur.fetchall():
                    grouped.setdefault(r["date"], []).append(r["product_id"])
                history[modelo] = [
                    {"date": d.isoformat(), "recommendations": products}
                    for d, products in grouped.items()
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
            # Consultas registradas, agrupadas por dia.
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

            # Cuantos advertisers distintos tiene cada modelo.
            cur.execute(
                "SELECT COUNT(DISTINCT advertiser_id) AS n "
                "FROM recommendations WHERE model = 'top_ctr'"
            )
            out["advertisers_top_ctr"] = cur.fetchone()["n"]

            cur.execute(
                "SELECT COUNT(DISTINCT advertiser_id) AS n "
                "FROM recommendations WHERE model = 'top_product'"
            )
            out["advertisers_top_product"] = cur.fetchone()["n"]

            # Para Jaccard: traemos todas las filas del dia mas reciente.
            # La subquery devuelve UNA fecha (la mas nueva de toda la tabla).
            cur.execute(
                """
                SELECT advertiser_id, model, product_id
                FROM recommendations
                WHERE date = (SELECT MAX(date) FROM recommendations)
                """
            )
            rows = cur.fetchall()

    # Agrupar productos por (advertiser, modelo) en sets.
    productos = defaultdict(lambda: {"top_ctr": set(), "top_product": set()})
    for r in rows:
        productos[r["advertiser_id"]][r["model"]].add(r["product_id"])

    coincidencias = []
    for advertiser_id, modelos in productos.items():
        ctr_set = modelos["top_ctr"]
        prod_set = modelos["top_product"]
        if not ctr_set or not prod_set:
            continue
        overlap = len(ctr_set & prod_set)
        union = len(ctr_set | prod_set)
        coincidencias.append(
            {
                "advertiser_id": advertiser_id,
                "productos_en_comun": overlap,
                "jaccard": round(overlap / union, 3) if union else 0,
            }
        )
    coincidencias.sort(key=lambda x: x["jaccard"], reverse=True)
    out["coincidencia_entre_modelos"] = coincidencias

    if not out["consultas_por_dia"]:
        out["mensaje_consultas"] = "No hay consultas registradas aun."
    return out
