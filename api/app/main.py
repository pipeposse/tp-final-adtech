from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import create_engine, text
from datetime import date
from contextlib import asynccontextmanager
import pandas as pd
import os
import ast

POSTGRES_URI = os.getenv("POSTGRES_URI")

if not POSTGRES_URI:
    raise RuntimeError("Falta la variable de entorno POSTGRES_URI")

engine = create_engine(POSTGRES_URI)

MODEL_TABLE_MAP = {
    "TopCTR": "top_ctr",
    "TopProduct": "top_product",
}


def create_logs_table():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS api_logs (
                id            SERIAL PRIMARY KEY,
                advertiser_id TEXT,
                model         TEXT,
                date          TEXT,
                timestamp     TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.commit()


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


@app.get("/recommendations/{advertiser_id}")
def get_recommendations(
    advertiser_id: str,
    model: str = Query(..., description="TopCTR or TopProduct"),
):
    if model not in MODEL_TABLE_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Modelo invalido: '{model}'. Opciones: {list(MODEL_TABLE_MAP.keys())}",
        )

    table = MODEL_TABLE_MAP[model]
    query = text(f"SELECT * FROM {table} WHERE advertiser_id = :adv_id")

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"adv_id": advertiser_id})

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No hay recomendaciones para advertiser '{advertiser_id}' con modelo '{model}'.",
        )

    with engine.connect() as conn:
        conn.execute(
            text("INSERT INTO api_logs (advertiser_id, model, date) VALUES (:adv, :model, :date)"),
            {"adv": advertiser_id, "model": model, "date": str(date.today())},
        )
        conn.commit()

    productos = df.iloc[0]["top_products"]
    if isinstance(productos, str):
        try:
            productos = ast.literal_eval(productos)
        except Exception:
            productos = [productos]

    return {
        "advertiser_id": advertiser_id,
        "model": model,
        "date": str(date.today()),
        "recommendations": productos,
    }


@app.get("/stats/")
def get_stats():
    query = text("""
        SELECT date,
               COUNT(DISTINCT advertiser_id) AS advertisers_consultados,
               COUNT(*) AS consultas_totales
        FROM api_logs
        GROUP BY date
        ORDER BY date DESC
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    if df.empty:
        return {"message": "No hay consultas registradas aun."}

    return df.to_dict(orient="records")
