# TP Final - AdTech Recommendations

Pipeline end-to-end de recomendacion de productos para advertisers, desplegado en Google Cloud Platform. Procesamiento diario de logs de impresiones y clicks, calculo de Top 20 productos por dos modelos (TopCTR y TopProduct), persistencia en PostgreSQL, y exposicion via API REST.

## Arquitectura

```
GCS bucket  (CSVs crudos diarios, en gs://tp-final-adtech/raw/)
   |
   v
Compute Engine VM (e2-small, us-central1-a)
  |-- Apache Airflow (scheduler + webserver)
  |   \-- DAG adtech_pipeline (@daily)
  |       |-- FiltrarDatos
  |       |-- top_ctr
  |       |-- top_product
  |       \-- DBWriting
  \-- Lee CSVs locales de /home/pipeposse/trabajo_practico/
   |
   v
Cloud SQL (PostgreSQL 15)
  |-- airflow_db   (metadata interna de Airflow)
  \-- recos_db
       |-- top_ctr      (resultado del ultimo run del DAG)
       |-- top_product  (resultado del ultimo run del DAG)
       \-- api_logs     (registro de consultas a la API)
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

Solo se procesan los advertisers que aparecen en el dataset `active_advertisers` del dia.

## Accesos para evaluacion

### API REST publica

URL base: `https://fastapi-tp-xbo6kajhza-uc.a.run.app`

| Endpoint | Descripcion |
|---|---|
| `GET /` | Status del servicio |
| `GET /health` | Health check |
| `GET /docs` | Documentacion interactiva (Swagger UI) |
| `GET /recommendations/{advertiser_id}/{modelo}` | Ultima recomendacion para un advertiser. Modelos: `TopCTR`, `TopProduct`. |
| `GET /history/{advertiser_id}/` | Recomendaciones disponibles, filtradas por ultimos 7 dias. Ver nota. |
| `GET /stats/` | Estadisticas: consultas por dia, advertisers por modelo, similaridad Jaccard entre modelos. |

> **Nota sobre `/history/`**: la query SQL filtra por los ultimos 7 dias, pero en la implementacion actual el DAG sobreescribe las tablas en cada run (`if_exists="replace"`), por lo que la base contiene solo el dia mas reciente. El endpoint devuelve un dia. Esta limitacion esta documentada en el informe (seccion 9).

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

DAG principal: `adtech_pipeline`. Schedule: diario.

## Stack tecnico

| Capa | Tecnologia |
|---|---|
| Orquestacion | Apache Airflow 2.x con LocalExecutor |
| Almacenamiento de datos | Cloud SQL (PostgreSQL 15) |
| API | FastAPI 0.115 + Uvicorn + psycopg2-binary |
| Containers | Docker, Cloud Build, Artifact Registry, Cloud Run |
| Storage de archivos crudos | Google Cloud Storage |
| Compute (Airflow) | Compute Engine VM (e2-small, us-central1-a) |
| Driver de DB en el DAG | pandas + SQLAlchemy (via to_sql) |
| Driver de DB en la API | psycopg2 directo |

## Estructura del repositorio

```
.
├── airflow_pipeline/
│   ├── dags/
│   │   └── recomendaciones_dag.py
│   └── requirements.txt
├── api/
│   ├── app/
│   │   ├── __init__.py
│   │   └── main.py             # endpoints
│   ├── Dockerfile
│   ├── requirements.txt
│   └── .dockerignore
├── docs/
│   └── informe.md              # informe completo en Markdown
├── sql/
│   └── schema.sql              # esquema descriptivo de las tablas
├── .gitignore
└── README.md
```

## Variables de entorno

El servicio FastAPI requiere `POSTGRES_URI` para conectarse a la base. Formato:

```
POSTGRES_URI=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

(El codigo tambien acepta el formato extendido `postgresql+psycopg2://...` y lo normaliza al estandar libpq antes de pasarlo a psycopg2.)

Las credenciales reales viven en Cloud Run environment variables y no estan commiteadas. El archivo `.env.example` documenta el formato esperado con un placeholder `TU_PASSWORD`.

## Despliegue

### API en Cloud Run

```bash
gcloud builds submit api/ \
  --tag=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:vN

gcloud run deploy fastapi-tp \
  --image=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:vN \
  --region=us-central1
```

### Airflow en VM

El DAG vive en `~/airflow/dags/recomendaciones_dag.py` dentro de la VM. El scheduler y webserver corren bajo el venv `~/airflow_venv/` con LocalExecutor contra `airflow_db` en Cloud SQL.

## Limitaciones conocidas

Documentadas en detalle en `docs/informe.md` (seccion 9):

- Las tablas se reescriben en cada run del DAG (`if_exists="replace"`), por lo que `/history/` devuelve solo el ultimo dia.
- TopCTR no aplica filtro de impresiones minimas; un producto con muestra chica puede tener CTR=1.0.
- El DAG lee CSVs locales, no descarga directamente de GCS. Los archivos se bajan manualmente con `gsutil cp`.
- El DAG usa SQLAlchemy via `pandas.to_sql`, mientras que la API usa psycopg2 directo. Asimetria heredada.
- El scheduler de Airflow corre con `nohup`, no con systemd. No se reinicia automaticamente al boot de la VM.

## Notas para la defensa

- El pipeline esta versionado por dia: cada corrida del DAG procesa el `ds` correspondiente y escribe en las tablas con esa fecha como columna.
- La API loguea cada consulta en `api_logs` para alimentar el endpoint `/stats/`.
- El analisis de similaridad Jaccard en `/stats/` muestra cuanto se solapan los dos modelos a nivel de productos recomendados por advertiser. Los valores observados (0.053 a 0.212) confirman que los modelos capturan senales distintas.
- La imagen Docker esta versionada por tag (v1, v2, ...) en Artifact Registry para permitir rollback.
