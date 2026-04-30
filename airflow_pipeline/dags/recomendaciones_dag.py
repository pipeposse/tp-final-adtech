"""
TP Final AdTech - DAG de recomendaciones diarias.

Pipeline:
    FiltrarDatos -> [TopCTR, TopProduct] -> DBWriting
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, date
import pandas as pd
from sqlalchemy import create_engine
import os

POSTGRES_URI = os.getenv("POSTGRES_URI")

if not POSTGRES_URI:
    raise RuntimeError(
        "Falta la variable de entorno POSTGRES_URI. "
        "Definirla en ~/.bashrc del usuario que corre Airflow."
    )

BASE_PATH = "/home/pipeposse/trabajo_practico"


def filtrar_datos(**context):
    hoy = str(date.today())

    active_advertisers = pd.read_csv(f"{BASE_PATH}/advertiser_ids.csv")
    log_product_views = pd.read_csv(f"{BASE_PATH}/product_views.csv")
    log_ads_views = pd.read_csv(f"{BASE_PATH}/ads_views.csv")

    log_product_views["date"] = log_product_views["date"].astype(str)
    log_ads_views["date"] = log_ads_views["date"].astype(str)

    product_views_filtrado = log_product_views[
        log_product_views["date"].str.startswith(hoy)
    ]
    product_views_filtrado = product_views_filtrado[
        product_views_filtrado["advertiser_id"].isin(
            active_advertisers["advertiser_id"]
        )
    ]

    ads_views_filtrado = log_ads_views[
        log_ads_views["date"].str.startswith(hoy)
    ]
    ads_views_filtrado = ads_views_filtrado[
        ads_views_filtrado["advertiser_id"].isin(
            active_advertisers["advertiser_id"]
        )
    ]

    product_views_filtrado.to_csv("/tmp/filtered_product_views.csv", index=False)
    ads_views_filtrado.to_csv("/tmp/filtered_ads_views.csv", index=False)

    print(f"Product views filtrados: {len(product_views_filtrado)}")
    print(f"Ads views filtrados: {len(ads_views_filtrado)}")


def top_ctr(**context):
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
        on=["advertiser_id", "product_id"],
        how="outer",
    ).fillna(0)

    counts["ctr"] = counts.apply(
        lambda row: row["clicks_count"] / row["impressions_count"]
        if row["impressions_count"] > 0 else 0,
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

    top_ctr_df.to_csv("/tmp/top_product_CTR.csv", index=False)
    print(f"TopCTR generado: {len(top_ctr_df)} advertisers")


def top_product(**context):
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

    top_product_df.to_csv("/tmp/top_product_VIEW.csv", index=False)
    print(f"TopProduct generado: {len(top_product_df)} advertisers")


def db_writing(**context):
    engine = create_engine(POSTGRES_URI)

    df_ctr = pd.read_csv("/tmp/top_product_CTR.csv")
    df_product = pd.read_csv("/tmp/top_product_VIEW.csv")

    df_ctr.to_sql("top_ctr", engine, if_exists="replace", index=False)
    df_product.to_sql("top_product", engine, if_exists="replace", index=False)

    print("Tablas top_ctr y top_product escritas en recos_db")


with DAG(
    dag_id="adtech_pipeline",
    description="TP Final - Recomendaciones diarias TopCTR + TopProduct",
    start_date=datetime(2026, 4, 28),
    schedule="@daily",
    catchup=False,
    tags=["tp-final", "adtech"],
) as dag:

    t1 = PythonOperator(task_id="FiltrarDatos", python_callable=filtrar_datos)
    t2 = PythonOperator(task_id="TopCTR", python_callable=top_ctr)
    t3 = PythonOperator(task_id="TopProduct", python_callable=top_product)
    t4 = PythonOperator(task_id="DBWriting", python_callable=db_writing)

    t1 >> [t2, t3] >> t4
