# TP Final — Pipeline de Recomendaciones AdTech

**Materia:** Programación Avanzada · Universidad de San Andrés · 2026
**Autores:** Felipe Posse · Diego Sanguinetti · Belén Candela Lozada Montanari
**Profesores:** Agustín Mosteiro · Matías Dinota
**Entrega:** 09/05/2026 · **Coloquio:** 13/05/2026

Sistema completo de recomendaciones AdTech que ingesta CSVs diarios desde Cloud Storage, los procesa con Apache Airflow, calcula dos modelos de ranking (TopCTR y TopProduct) y los expone vía API REST en Cloud Run.

---

## Acceso rápido

| Recurso | URL |
|---|---|
| API REST (Cloud Run) | https://fastapi-tp-xbo6kajhza-uc.a.run.app |
| Documentación interactiva (Swagger) | https://fastapi-tp-xbo6kajhza-uc.a.run.app/docs |
| Airflow UI | http://34.55.205.251:8080 (usuario: `profesor` / pass: `profesor`, rol Viewer) |
| Informe técnico completo | [`docs/INFORME_FINAL.md`](docs/INFORME_FINAL.md) |

---

## Arquitectura

```
GCS (gs://tp-final-adtech/)
        │
        ▼
Compute Engine VM (airflow-vm, e2-small, us-central1-a)
   ├── Apache Airflow scheduler + webserver
   └── DAG: adtech_pipeline_v2_gcs
         FiltrarDatos → TopCTR + TopProduct → DBWriting
                                                    │
                                                    ▼
                            Cloud SQL Postgres 18 (recos_db)
                                ├── recommendations
                                └── api_logs
                                                    ▲
                                                    │
                            Cloud Run (fastapi-tp:v3)
                                        ▲
                                        │
                                    Cliente HTTP
```

El DAG corre diariamente a las **02:00 UTC**. La API es stateless y autoescala con tráfico.

---

## Identificadores GCP

| Recurso | Valor |
|---|---|
| Project ID | `tp-final-adtech-493922` |
| Region | `us-central1` |
| Cloud SQL instance | `tp-final-adtech-493922` |
| Postgres IP pública | `34.46.239.72` |
| Bases en Cloud SQL | `airflow_db` (metadata Airflow) · `recos_db` (recomendaciones) |
| Bucket de datos | `gs://tp-final-adtech/` |
| VM Airflow | `airflow-vm` (zone `us-central1-a`, IP estática `34.55.205.251`) |
| Artifact Registry repo | `tp-final-repo` (region `us-central1`) |
| Imagen API activa | `us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:v3` |
| Servicio Cloud Run | `fastapi-tp` |

---

## Estructura del repositorio

```
tp-final-adtech/
├── airflow_pipeline/
│   ├── dags/recomendaciones_dag.py    # DAG adtech_pipeline_v2_gcs
│   ├── utils/{gcs_io.py, db.py}        # Helpers de GCS y Postgres
│   └── requirements.txt
├── api/
│   ├── app/main.py                     # FastAPI (psycopg2 directo)
│   ├── Dockerfile                      # python:3.12-slim, uvicorn
│   └── requirements.txt
├── sql/
│   └── schema.sql                      # Schema de recos_db
├── docs/
│   └── INFORME_FINAL.md                # Informe técnico del TP
└── README.md
```

---

## Modelo de datos

Schema final en `recos_db` (definido en [`sql/schema.sql`](sql/schema.sql)):

```sql
CREATE TABLE recommendations (
    id            BIGSERIAL PRIMARY KEY,
    advertiser_id VARCHAR(64)  NOT NULL,
    model         VARCHAR(32)  NOT NULL,    -- 'top_ctr' | 'top_product'
    product_id    VARCHAR(128) NOT NULL,
    rank          INTEGER      NOT NULL,
    score         NUMERIC(10,6),
    date          DATE         NOT NULL,
    created_at    TIMESTAMP    DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_recommendations_natural_key
    ON recommendations (advertiser_id, model, product_id, date);
```

El índice único es necesario para que el DAG pueda hacer `INSERT ... ON CONFLICT ... DO UPDATE`. La tabla `api_logs` la crea la API automáticamente al arrancar.

---

## Endpoints de la API

| Método | Endpoint | Descripción |
|---|---|---|
| `GET` | `/health` | Health check sin tocar la base |
| `GET` | `/recommendations/{advertiser_id}/{modelo}` | Top 20 productos del último día. `modelo` ∈ {`TopCTR`, `TopProduct`} |
| `GET` | `/history/{advertiser_id}/` | Recomendaciones de los últimos 7 días, agrupadas por día |
| `GET` | `/stats/` | Consultas por día, advertisers por modelo, similaridad Jaccard entre modelos |
| `GET` | `/docs` | Swagger UI |

Códigos HTTP: `200` OK · `400` modelo inválido · `404` advertiser sin datos · `500` error interno.

### Ejemplos rápidos

```bash
BASE=https://fastapi-tp-xbo6kajhza-uc.a.run.app
ADV=OAGTYWN8WFC997VLDJH7   # advertiser con mayor Jaccard (0.333)

curl -s $BASE/health
curl -s $BASE/recommendations/$ADV/TopCTR
curl -s $BASE/recommendations/$ADV/TopProduct
curl -s $BASE/history/$ADV/
curl -s $BASE/stats/
```

---

## Cómo levantar todo desde cero

El walkthrough completo está en [`docs/INFORME_FINAL.md`](docs/INFORME_FINAL.md). Resumen operativo:

### 1. Cloud SQL

Crear la base `recos_db` y aplicar el schema:

```bash
psql -h 34.46.239.72 -U postgres -d recos_db -f sql/schema.sql
```

### 2. VM de Airflow

SSH a `airflow-vm`, activar el venv y disparar el scheduler/webserver:

```bash
gcloud compute ssh airflow-vm --zone us-central1-a --project tp-final-adtech-493922
source ~/airflow_venv/bin/activate

# Asegurar la env var en la shell antes de lanzar:
export POSTGRES_URI='postgresql+psycopg2://USER:PASS@34.46.239.72:5432/recos_db'

nohup airflow scheduler  > ~/airflow/scheduler.log 2>&1 & disown
nohup airflow webserver -p 8080 > ~/airflow/webserver.log 2>&1 & disown
```

### 3. API en Cloud Run

Build de la imagen + deploy:

```bash
cd api/
gcloud builds submit \
  --tag us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:vN \
  --project tp-final-adtech-493922

gcloud run deploy fastapi-tp \
  --image us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:vN \
  --region us-central1 \
  --project tp-final-adtech-493922 \
  --set-env-vars 'POSTGRES_URI=postgresql+psycopg2://USER:PASS@34.46.239.72:5432/recos_db'
```

> El bloque `--set-env-vars` debe ir entre comillas simples para que el shell no se coma los caracteres especiales del password URL-encoded.

### 4. Verificación

```bash
curl -s https://fastapi-tp-xbo6kajhza-uc.a.run.app/health
# {"status":"ok"}
```

---

## Tecnologías utilizadas

- **Python 3.12** — pandas, psycopg2, FastAPI, uvicorn
- **Apache Airflow** (LocalExecutor, sobre VM e2-small)
- **PostgreSQL 18** (Cloud SQL administrado)
- **Docker** + **Cloud Build** + **Artifact Registry**
- **Cloud Run** (serverless containers)
- **Google Cloud Storage** (data lake de CSVs)

---

## Notas de versionado

| Componente | Versión actual | Notas |
|---|---|---|
| API (Cloud Run) | `fastapi-tp:v3` | Migración al schema normalizado (2026-05-09) |
| DAG | `adtech_pipeline_v2_gcs` | Lectura directa de GCS, upsert con ON CONFLICT |
| Schema SQL | normalizado | Ver `sql/schema.sql` |

Para detalle de cambios y problemas resueltos, ver [`docs/INFORME_FINAL.md`](docs/INFORME_FINAL.md) (secciones 7 y 8).
