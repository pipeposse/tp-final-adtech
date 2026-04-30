"""
TP Final AdTech - DAG de recomendaciones diarias.

Cada DAG run procesa los datos de su `logical_date` (Airflow context["ds"]):
    FiltrarDatos(ds) -> [TopCTR(ds), TopProduct(ds)] -> DBWriting(ds)

Los CSVs vienen ya particionados por día:
    ads_views_YYYY-MM-DD.csv
    product_views_YYYY-MM-DD.csv
    active_advertisers.csv  (no particionado)

Las tablas finales en recos_db se reescriben con el resultado del último run.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import os

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

POSTGRES_URI = os.getenv("POSTGRES_URI")

if not POSTGRES_URI:
    raise RuntimeError(
        "Falta la variable de entorno POSTGRES_URI. "
        "Definirla en ~/.bashrc del usuario que corre Airflow."
    )

BASE_PATH = "/home/pipeposse/trabajo_practico"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

def filtrar_datos(ds, **context):
    """Lee los CSVs del día `ds` y filtra solo advertisers activos.
    Guarda intermedios en /tmp para que las tasks siguientes los lean."""
    active_advertisers = pd.read_csv(f"{BASE_PATH}/active_advertisers.csv")
    log_ads_views = pd.read_csv(f"{BASE_PATH}/ads_views_{ds}.csv")
    log_product_views = pd.read_csv(f"{BASE_PATH}/product_views_{ds}.csv")

    activos = active_advertisers["advertiser_id"]

    ads_views_filtrado = log_ads_views[log_ads_views["advertiser_id"].isin(activos)]
    product_views_filtrado = log_product_views[
        log_product_views["advertiser_id"].isin(activos)
    ]

    ads_views_filtrado.to_csv("/tmp/filtered_ads_views.csv", index=False)
    product_views_filtrado.to_csv("/tmp/filtered_product_views.csv", index=False)

    print(f"[ds={ds}] ads_views filtrados: {len(ads_views_filtrado)}")
    print(f"[ds={ds}] product_views filtrados: {len(product_views_filtrado)}")


def top_ctr(ds, **context):
    """Top 20 productos por advertiser ordenados por CTR (clicks/impresiones)."""
    log_ads = pd.read_csv("/tmp/filtered_ads_views.csv")

    clicks = (
        log_ads[log_ads["type"] == "click"]
        .groupby(["advertiser_id", "product_id"])
        .size()
        .reset_index(name="clicks_count")
    )
    impressions = (
        log_ads[log_ads["type"] == "impression"]
        .groupby(["advertiser_id", "product_id"])
        .size()
        .reset_index(name="impressions_count")
    )

    counts = pd.merge(
        clicks, impressions,
        on=["advertiser_id", "product_id"], how="outer",
    ).fillna(0)

    counts["ctr"] = counts.apply(
        lambda r: r["clicks_count"] / r["impressions_count"]
        if r["impressions_count"] > 0 else 0,
        axis=1,
    )

    top_ctr_df = (
        counts.sort_values(["advertiser_id", "ctr"], ascending=[True, False])
        .groupby("advertiser_id")
        .head(20)
        .groupby("advertiser_id")["product_id"]
        .apply(list)
        .reset_index(name="top_products")
    )
    top_ctr_df["date"] = ds

    top_ctr_df.to_csv("/tmp/top_product_CTR.csv", index=False)
    print(f"[ds={ds}] TopCTR generado: {len(top_ctr_df)} advertisers")


def top_product(ds, **context):
    """Top 20 productos por advertiser ordenados por cantidad de views."""
    log_product_views = pd.read_csv("/tmp/filtered_product_views.csv")

    counts = (
        log_product_views
        .groupby(["advertiser_id", "product_id"])
        .size()
        .reset_index(name="views")
    )

    top_product_df = (
        counts.sort_values(["advertiser_id", "views"], ascending=[True, False])
        .groupby("advertiser_id")
        .head(20)
        .groupby("advertiser_id")["product_id"]
        .apply(list)
        .reset_index(name="top_products")
    )
    top_product_df["date"] = ds

    top_product_df.to_csv("/tmp/top_product_VIEW.csv", index=False)
    print(f"[ds={ds}] TopProduct generado: {len(top_product_df)} advertisers")


def db_writing(ds, **context):
    """Vuelca los dos rankings a Postgres (Cloud SQL)."""
    engine = create_engine(POSTGRES_URI)

    df_ctr = pd.read_csv("/tmp/top_product_CTR.csv")
    df_product = pd.read_csv("/tmp/top_product_VIEW.csv")

    df_ctr.to_sql("top_ctr", engine, if_exists="replace", index=False)
    df_product.to_sql("top_product", engine, if_exists="replace", index=False)

    print(f"[ds={ds}] tablas top_ctr y top_product reescritas en recos_db")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="adtech_pipeline",
    description="TP Final - Recomendaciones diarias TopCTR + TopProduct",
    start_date=datetime(2026, 4, 18),
    schedule="@daily",
    catchup=False,
    tags=["tp-final", "adtech"],
) as dag:

    t1 = PythonOperator(task_id="FiltrarDatos", python_callable=filtrar_datos)
    t2 = PythonOperator(task_id="TopCTR", python_callable=top_ctr)
    t3 = PythonOperator(task_id="TopProduct", python_callable=top_product)
    t4 = PythonOperator(task_id="DBWriting", python_callable=db_writing)

    t1 >> [t2, t3] >> t4
