# TP Final - AdTech Recommendations

Pipeline end-to-end de recomendacion de productos para advertisers, desplegado en Google Cloud Platform. Ingesta diaria de logs de impresiones y clicks, calculo de Top 20 productos por dos modelos (TopCTR y TopProduct), persistencia en PostgreSQL, y exposicion via API REST.

## Arquitectura

```
GCS (CSVs diarios)
   |
   v
Airflow DAG (Compute Engine VM)
   |  - descarga
   |  - filtra advertisers elegibles
   |  - calcula TopCTR
   |  - calcula TopProduct
   v
Cloud SQL (PostgreSQL)
   |  - top_ctr
   |  - top_product
   |  - api_logs
   ^
   |
FastAPI service (Cloud Run)
   |
   v
Cliente HTTP (profesor / corrector)
```

## Modelos de recomendacion

- **TopCTR**: Top 20 productos por advertiser ordenados por click-through ratio (clicks/impresiones).
- **TopProduct**: Top 20 productos por advertiser ordenados por cantidad de visualizaciones.

Solo se procesan los advertisers que aparecen en el dataset `eligible_advertisers` del dia.

## Accesos para evaluacion

### API REST publica

URL base: `https://fastapi-tp-xbo6kajhza-uc.a.run.app`

| Endpoint | Descripcion |
|---|---|
| `GET /health` | Health check |
| `GET /recommendations/{advertiser_id}/{modelo}` | Ultima recomendacion para un advertiser. Modelos: `TopCTR`, `TopProduct`. |
| `GET /history/{advertiser_id}/` | Recomendaciones de los ultimos 7 dias para ambos modelos. |
| `GET /stats/` | Estadisticas: consultas por dia, advertisers por modelo, similaridad Jaccard entre modelos. |

Ejemplos:

```bash
curl https://fastapi-tp-xbo6kajhza-uc.a.run.app/health
curl https://fastapi-tp-xbo6kajhza-uc.a.run.app/recommendations/LW045DVYSGRD75TK6U54/TopCTR
curl https://fastapi-tp-xbo6kajhza-uc.a.run.app/history/LW045DVYSGRD75TK6U54/
curl https://fastapi-tp-xbo6kajhza-uc.a.run.app/stats/
```

### Airflow UI

URL: `http://34.55.205.251:8080`

Credenciales de evaluacion (rol Viewer, solo lectura):

```
Usuario:  profesor
Password: profesor
```

DAG principal: `adtech_recos`. Schedule: diario.

## Stack tecnico

| Capa | Tecnologia |
|---|---|
| Orquestacion | Apache Airflow 2.x con LocalExecutor |
| Almacenamiento de datos | Cloud SQL (PostgreSQL 15) |
| API | FastAPI 0.115 + Uvicorn + psycopg2-binary |
| Containers | Docker, Cloud Build, Artifact Registry, Cloud Run |
| Storage de archivos crudos | Google Cloud Storage |
| Compute (Airflow) | Compute Engine VM (e2-small, us-central1-a) |

## Estructura del repositorio

```
.
├── airflow/                    # DAG y modulos auxiliares
│   └── dags/
│       └── adtech_recos.py
├── api/                        # Servicio FastAPI
│   ├── app/
│   │   └── main.py             # endpoints
│   ├── Dockerfile
│   ├── requirements.txt
│   └── .dockerignore
├── .env.example                # template de variables de entorno
├── .gitignore
└── README.md
```

## Variables de entorno

El servicio FastAPI requiere `POSTGRES_URI` para conectarse a la base. Formato:

```
POSTGRES_URI=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

(El codigo tambien acepta el formato SQLAlchemy `postgresql+psycopg2://...` y lo normaliza al estandar libpq.)

Las credenciales reales viven en Cloud Run environment variables y no estan commiteadas. El archivo `.env.example` documenta el formato esperado.

## Despliegue

### API en Cloud Run

```bash
gcloud builds submit api/ \
  --tag=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:vX

gcloud run deploy fastapi-tp \
  --image=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:vX \
  --region=us-central1
```

### Airflow en VM

El DAG se sincroniza por `git pull` desde la VM hacia `~/airflow/dags/`. El scheduler y webserver corren bajo el venv `~/airflow_venv/` con LocalExecutor contra `airflow_db` en Cloud SQL.

## Notas para la defensa

- El pipeline esta versionado por dia: cada corrida del DAG escribe en las tablas con su `date` correspondiente.
- La API loguea cada consulta en `api_logs` para alimentar el endpoint `/stats/`.
- El analisis de similaridad Jaccard en `/stats/` muestra cuanto se solapan los dos modelos a nivel de productos recomendados por advertiser.
- La imagen Docker esta versionada por tag (v1, v2, ...) en Artifact Registry para permitir rollback.
