# Guía paso a paso — TP Final AdTech

**Programación Avanzada · UDESA · 2026**

Esta guía cuenta cómo armar el sistema desde cero. Está pensada para que cualquiera del equipo (o cualquier curioso) pueda seguirla, entender qué hace cada pieza, defenderlo en el coloquio, y reproducirlo si hace falta. **No damos nada por sentado.** Si una parte parece obvia, la explicamos igual.

Importante: la guía describe el sistema **tal como está implementado hoy**, con sus decisiones reales y sus limitaciones. Las cosas que decidimos no implementar o que dejamos para una mejora futura aparecen claramente marcadas como tales.

---

## Tabla de contenidos

```
0.  Antes de arrancar — concepto general
1.  Crear el proyecto en Google Cloud
2.  Cloud Storage — donde viven los datos crudos
3.  Cloud SQL — la base PostgreSQL
4.  Compute Engine — la máquina virtual para Airflow
5.  Instalar y levantar Airflow
6.  Escribir el DAG (el pipeline)
7.  La API con FastAPI
8.  Docker — empaquetar la API
9.  Cloud Run — desplegar la API en internet
10. IP estática para Airflow
11. Usuario profesor en Airflow
12. Repositorio Git público
13. Errores que nos pasaron y cómo los resolvimos
14. Cómo mantener todo vivo entre la entrega y el coloquio
15. Glosario — todos los términos en castellano
```

---

# 0. Antes de arrancar — concepto general

## ¿Qué estamos construyendo?

Un sistema de recomendación de productos para anunciantes. La idea:

```
   Cada día caen archivos con datos de impresiones y clicks
              │
              ▼
   Un proceso los toma, los procesa, calcula
   "los 20 productos top" para cada anunciante
              │
              ▼
   Esos resultados se guardan en una base
              │
              ▼
   Una API web los expone, así cualquiera con la URL
   puede consultar las recomendaciones
```

Los archivos viven en **Google Cloud Storage** (GCS), el procesamiento lo orquesta **Apache Airflow** corriendo en una máquina virtual de **Compute Engine**, los resultados se guardan en una base PostgreSQL administrada (**Cloud SQL**), y la API la sirve **Cloud Run** desde una imagen Docker.

## Las cinco piezas, en una foto

```
   ┌─────────────────┐
   │  Cloud Storage  │   ← bucket con CSVs diarios
   └────────┬────────┘
            │  (descarga manual con gsutil cp)
            ▼
   ┌─────────────────────────────┐
   │   Compute Engine VM          │
   │   (e2-small, Linux)          │
   │   ├── Airflow scheduler      │
   │   ├── Airflow webserver      │
   │   └── DAG diario             │
   │       ├─ FiltrarDatos        │
   │       ├─ top_ctr             │
   │       ├─ top_product         │
   │       └─ DBWriting           │
   └─────────┬───────────────────┘
             │
             ▼
   ┌─────────────────┐
   │   Cloud SQL     │   ← Postgres con dos bases:
   │   (PostgreSQL)  │     airflow_db, recos_db
   └────────▲────────┘
            │
   ┌────────┴────────┐
   │   Cloud Run     │   ← FastAPI serving HTTPS
   │   (Docker)      │
   └────────▲────────┘
            │
            │
   Profe / corrector / cliente HTTP
```

## ¿Qué necesitás saber antes?

- **Linux básico**: moverte por la terminal, editar archivos con `nano`.
- **Python**: qué es un `venv`, cómo instalar paquetes con `pip`.
- **SQL**: `CREATE TABLE`, `INSERT`, `SELECT`.
- **HTTP**: GET, códigos 200/400/404/500, JSON.
- **Git**: clonar, commit, push.
- **Docker conceptual**: imagen vs container.

---

# 1. Crear el proyecto en Google Cloud

## 1.1 Tener cuenta de Google Cloud

Andá a https://console.cloud.google.com con tu cuenta de Google. Te dan 300 USD de crédito gratis.

## 1.2 Crear el proyecto

Arriba a la izquierda → **Nuevo proyecto**. Nombre cualquiera (ej. `tp-final-adtech`). **Anotá el Project ID** que genera Google (ej. `tp-final-adtech-493922`). Lo vas a usar en todos los comandos.

## 1.3 Habilitar las APIs

Menú ≡ → **APIs y servicios** → **Biblioteca**. Habilitá:

- Compute Engine API
- Cloud SQL Admin API
- Cloud Storage API
- Cloud Run Admin API
- Artifact Registry API
- Cloud Build API
- IAM Service Account Credentials API

## 1.4 Una sola región para todo

Mezclar regiones es problema. Elegimos `us-central1` y nos pegamos ahí.

---

# 2. Cloud Storage — donde viven los datos crudos

## 2.1 Crear el bucket

Menú ≡ → **Cloud Storage** → **Buckets** → **CREATE**.

```
Nombre:       tp-final-adtech (o lo que prefieras, único)
Tipo:         Region
Región:       us-central1
Storage class: Standard
Access:       Uniform
```

## 2.2 Subir los CSVs

El TP usa tres datasets:

```
active_advertisers.csv          (no particionado)
ads_views_YYYY-MM-DD.csv        (uno por día)
product_views_YYYY-MM-DD.csv    (uno por día)
```

Carpeta dentro del bucket:

```
gs://tp-final-adtech/
└── raw/
    ├── active_advertisers.csv
    ├── ads_views_2026-04-18.csv
    └── product_views_2026-04-18.csv
    ...
```

---

# 3. Cloud SQL — la base PostgreSQL

## 3.1 Crear la instancia

**Tarda 8-10 minutos.** Lanzala ahora y mientras seguí con los pasos siguientes.

Menú ≡ → **SQL** → **CREATE INSTANCE** → **PostgreSQL**.

```
Instance ID:        tp-final-adtech-493922
Password postgres:  (algo robusto, ANOTALO)
Database version:   PostgreSQL 15
Región:             us-central1
Machine type:       db-f1-micro
Storage:            10 GB SSD
Connections:        Public IP
```

## 3.2 Anotar datos de conexión

```
IP pública:       (la tuya)
Connection name:  PROJECT:REGION:INSTANCE
```

## 3.3 Crear las dos bases

Dentro de la instancia → **Databases** → **CREATE DATABASE**:

```
airflow_db   ← metadata interna de Airflow
recos_db     ← tablas del proyecto
```

## 3.4 Esquema (informativo)

**Las tablas se crean solas en runtime**, no las tenés que crear a mano:

- `top_ctr` y `top_product`: las crea el DAG con `pandas.to_sql`. Como el DAG usa `if_exists="replace"`, en cada run se borran y recrean enteras. Esto es importante: la base nunca tiene más de un día de datos. Lo tratamos en la sección 13 como limitación.

- `api_logs`: la crea la API al arrancar (en el handler de `lifespan`).

---

# 4. Compute Engine — la máquina virtual para Airflow

## 4.1 Concepto

Compute Engine es máquinas virtuales en la nube. ¿Por qué VM y no Cloud Run para Airflow? Porque Airflow tiene que estar siempre prendido (su scheduler revisa cada pocos segundos si toca disparar un DAG, aunque nadie lo consulte).

## 4.2 Crear la VM

Menú ≡ → **Compute Engine** → **VM instances** → **CREATE INSTANCE**.

```
Name:           airflow-vm
Region/Zone:    us-central1 / us-central1-a
Machine type:   e2-small
Boot disk:      Ubuntu 22.04 LTS, 20 GB
Firewall:       Allow HTTP, Allow HTTPS
Service acct:   default con full access
```

## 4.3 Abrir el puerto 8080

Menú ≡ → **VPC network** → **Firewall** → **CREATE FIREWALL RULE**:

```
Name:               allow-airflow-ui
Direction:          Ingress
Action:             Allow
Targets:            All instances
Source IPv4:        0.0.0.0/0
Protocols/ports:    TCP 8080
```

## 4.4 Entrar por SSH

En la lista de VMs, botón **SSH**. Se abre una terminal en el navegador.

---

# 5. Instalar y levantar Airflow

Todo dentro de la VM.

## 5.1 Paquetes base

```bash
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv libpq-dev postgresql-client git
```

## 5.2 Verificar conexión a la base

```bash
psql "host=IP_DE_LA_INSTANCIA port=5432 user=postgres dbname=airflow_db sslmode=require"
```

## 5.3 Venv y AIRFLOW_HOME

```bash
cd ~
python3 -m venv airflow_venv
source airflow_venv/bin/activate

export AIRFLOW_HOME=~/airflow
echo 'export AIRFLOW_HOME=~/airflow' >> ~/.bashrc
echo 'source ~/airflow_venv/bin/activate' >> ~/.bashrc
```

## 5.4 Setear POSTGRES_URI

```bash
echo 'export POSTGRES_URI="postgresql://postgres:TU_PASSWORD@IP:5432/recos_db"' >> ~/.bashrc
source ~/.bashrc
```

## 5.5 Instalar Airflow

```bash
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION="$(python --version | cut -d ' ' -f2 | cut -d '.' -f1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" \
  "apache-airflow-providers-postgres" \
  --constraint "${CONSTRAINT_URL}"

pip install pandas psycopg2-binary sqlalchemy
```

> **Nota:** SQLAlchemy se instala porque el DAG la necesita (la usa indirectamente a través de `pandas.to_sql`, que requiere un engine). Más detalles en la sección 6.

## 5.6 Configurar airflow.cfg

```bash
nano $AIRFLOW_HOME/airflow.cfg
```

Cambiar:

```ini
[core]
executor = LocalExecutor
parallelism = 2
load_examples = False

[database]
sql_alchemy_conn = postgresql://postgres:TU_PASSWORD@IP:5432/airflow_db
```

## 5.7 Inicializar la base de Airflow y crear admin

```bash
airflow db migrate
airflow users create --username admin --firstname Pipe --lastname Posse \
  --role Admin --email tu@email.com --password admin123
```

## 5.8 Levantar webserver y scheduler

```bash
airflow webserver -p 8080 -D
airflow scheduler -D
```

> **Importante**: estos comandos arrancan los procesos como daemons (background). Si la VM se reinicia, no se levantan solos. Lo prolijo es crear servicios systemd, pero por tiempo lo dejamos como mejora pendiente (sección 13).

## 5.9 Verificar

Abrí en el navegador:

```
http://IP_EXTERNA_VM:8080
```

Login con `admin / admin123`. Tendrías que ver la lista de DAGs (vacía por ahora).

---

# 6. Escribir el DAG (el pipeline)

## 6.1 Concepto

Un DAG es un Python que define tareas y sus dependencias. Airflow se encarga del orden, los reintentos y los logs.

## 6.2 Estructura de nuestro DAG

```
   ┌──────────────┐
   │ FiltrarDatos │   1. Lee CSVs locales del día, filtra por
   └──────┬───────┘      active_advertisers, guarda en /tmp
          │
   ┌──────┴───────┐
   ▼              ▼
┌──────┐    ┌────────────┐
│TopCTR│    │ TopProduct │  2. y 3. corren en paralelo, leen
└───┬──┘    └──────┬─────┘     de /tmp, calculan rankings
    │              │
    └──────┬───────┘
           ▼
   ┌──────────────┐
   │  DBWriting   │   4. Escribe ambos resultados a Postgres
   └──────────────┘
```

## 6.3 El código del DAG

Crear el archivo:

```bash
mkdir -p $AIRFLOW_HOME/dags
nano $AIRFLOW_HOME/dags/recomendaciones_dag.py
```

### Imports y configuración

```python
"""
TP Final AdTech - DAG de recomendaciones diarias.
Cada DAG run procesa los datos de su `logical_date` (Airflow context["ds"]):
    FiltrarDatos(ds) -> [TopCTR(ds), TopProduct(ds)] -> DBWriting(ds)
Las tablas finales en recos_db se reescriben con el resultado del último run.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
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
```

> **Nota:** el DAG espera CSVs locales en `BASE_PATH`. Los bajamos con `gsutil cp` desde el bucket. Una mejora pendiente es que el DAG los descargue solo desde GCS.

### Tarea 1 — FiltrarDatos

```python
def filtrar_datos(ds, **context):
    """Lee los CSVs del día `ds` y filtra solo advertisers activos.
    Guarda intermedios en /tmp para que las tareas siguientes los lean."""
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
```

**Conceptos clave:**
- `ds` es la fecha lógica del run, formato `"YYYY-MM-DD"`. La inyecta Airflow automáticamente.
- El nombre de archivo se construye con esa fecha: `ads_views_2026-04-19.csv`.
- Los archivos intermedios van a `/tmp` (mismo filesystem de la VM).

### Tarea 2 — top_ctr

```python
def top_ctr(ds, **context):
    """Top 20 productos por advertiser ordenados por CTR (clicks/impresiones)."""
    log_ads = pd.read_csv("/tmp/filtered_ads_views.csv")
    
    clicks = (log_ads[log_ads["type"] == "click"]
              .groupby(["advertiser_id", "product_id"])
              .size().reset_index(name="clicks_count"))
    impressions = (log_ads[log_ads["type"] == "impression"]
                   .groupby(["advertiser_id", "product_id"])
                   .size().reset_index(name="impressions_count"))
    
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
```

**Lo que hace:**
1. Cuenta clicks y impresiones por par (advertiser, producto).
2. Calcula CTR = clicks / impresiones.
3. Para cada advertiser, agarra los 20 productos con mayor CTR.
4. Convierte la lista a una sola fila con columna `top_products`.

> **Limitación conocida:** el `if r["impressions_count"] > 0 else 0` evita división por cero, pero NO descarta productos con pocas impresiones. Un producto con 2 impresiones y 2 clicks tiene CTR=1.0 y puede ganar el ranking. Una mejora directa sería agregar un filtro `impressions_count >= N` antes de calcular el CTR. Lo dejamos como mejora pendiente (sección 13).

### Tarea 3 — top_product

```python
def top_product(ds, **context):
    """Top 20 productos por advertiser ordenados por cantidad de views."""
    log_product_views = pd.read_csv("/tmp/filtered_product_views.csv")
    
    counts = (log_product_views
              .groupby(["advertiser_id", "product_id"])
              .size().reset_index(name="views"))
    
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
```

Más simple que TopCTR: cuenta visualizaciones y se queda con el top 20.

### Tarea 4 — DBWriting

```python
def db_writing(ds, **context):
    """Vuelca los dos rankings a Postgres (Cloud SQL)."""
    engine = create_engine(POSTGRES_URI)
    df_ctr = pd.read_csv("/tmp/top_product_CTR.csv")
    df_product = pd.read_csv("/tmp/top_product_VIEW.csv")
    df_ctr.to_sql("top_ctr", engine, if_exists="replace", index=False)
    df_product.to_sql("top_product", engine, if_exists="replace", index=False)
    print(f"[ds={ds}] tablas top_ctr y top_product reescritas en recos_db")
```

**Conceptos importantes:**
- `create_engine(POSTGRES_URI)` crea un engine de SQLAlchemy. Es lo que `pandas.to_sql` necesita: pandas no habla con Postgres directamente, sino que delega en SQLAlchemy.
- `if_exists="replace"` significa: si la tabla existe, BORRARLA y recrearla. Cada run del DAG **borra y reescribe enteras** las dos tablas.

> **Limitación importante:** con `if_exists="replace"`, la base nunca tiene más de un día de datos. Esto rompe la promesa del endpoint `/history/{advertiser_id}/` (los últimos 7 días): en la práctica, el endpoint devuelve solo el último día. La mejora directa sería usar `if_exists="append"` después de un `DELETE FROM ... WHERE date = ds`, manteniendo histórico real. Lo dejamos como mejora pendiente (sección 13).

> **Nota sobre asimetría:** la API se conecta a Postgres con psycopg2 directo (sin ORM), pero el DAG usa SQLAlchemy a través de pandas. Es una asimetría heredada. Una mejora prolija sería unificar ambos componentes en psycopg2 directo, pero por tiempo lo dejamos así.

### Ensamble del DAG

```python
with DAG(
    dag_id="adtech_pipeline",
    description="TP Final - Recomendaciones diarias TopCTR + TopProduct",
    start_date=datetime(2026, 4, 18),
    schedule="@daily",
    catchup=False,
    tags=["tp-final", "adtech"],
) as dag:
    t1 = PythonOperator(task_id="FiltrarDatos", python_callable=filtrar_datos)
    t2 = PythonOperator(task_id="top_ctr", python_callable=top_ctr)
    t3 = PythonOperator(task_id="top_product", python_callable=top_product)
    t4 = PythonOperator(task_id="DBWriting", python_callable=db_writing)
    
    t1 >> [t2, t3] >> t4
```

**Conceptos:**
- `dag_id="adtech_pipeline"`: el nombre interno del DAG.
- `schedule="@daily"`: el scheduler dispara un run por día.
- `catchup=False`: no procesar runs viejos cuando se prende el DAG.

## 6.4 Bajar los CSVs del bucket a la VM

```bash
mkdir -p ~/trabajo_practico
gsutil -m cp gs://tp-final-adtech/raw/*.csv ~/trabajo_practico/
```

## 6.5 Probar el DAG manualmente

```bash
airflow tasks test adtech_pipeline FiltrarDatos 2026-04-18
airflow tasks test adtech_pipeline top_ctr 2026-04-18
airflow tasks test adtech_pipeline top_product 2026-04-18
airflow tasks test adtech_pipeline DBWriting 2026-04-18
```

Si las cuatro terminan con `Marking task as SUCCESS`, todo bien. En la UI de Airflow, prendé el toggle del DAG.

---

# 7. La API con FastAPI

## 7.1 Estructura

En tu máquina local:

```
api/
├── app/
│   ├── __init__.py    (vacío)
│   └── main.py        (los endpoints)
├── Dockerfile
├── requirements.txt
└── .dockerignore
```

## 7.2 requirements.txt

```
fastapi==0.115.0
uvicorn[standard]==0.30.6
psycopg2-binary==2.9.10
```

## 7.3 main.py — código real

El código completo vive en el repo. Lo más importante:

### Normalización de la URI

```python
POSTGRES_URI = os.getenv("POSTGRES_URI")
if not POSTGRES_URI:
    raise RuntimeError("Falta la variable de entorno POSTGRES_URI")

# psycopg2 directo no entiende el dialecto SQLAlchemy.
# Lo normalizamos al formato estandar.
if POSTGRES_URI.startswith("postgresql+psycopg2://"):
    POSTGRES_URI = POSTGRES_URI.replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )
```

> Esta normalización la agregamos cuando descubrimos que la URI seteada en Cloud Run venía con prefijo extendido. Detalle completo en la sección 13.

### Conexión a la base

```python
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
```

### Endpoint de recomendaciones

```python
@app.get("/recommendations/{advertiser_id}/{modelo}")
def get_recommendations(advertiser_id: str = Path(...), modelo: str = Path(...)):
    if modelo not in MODEL_TABLE_MAP:
        raise HTTPException(400, "Modelo invalido...")
    table = MODEL_TABLE_MAP[modelo]
    
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT advertiser_id, top_products, date "
                f"FROM {table} WHERE advertiser_id = %s "
                f"ORDER BY date DESC LIMIT 1",
                (advertiser_id,)
            )
            row = cur.fetchone()
    # ... 404 si no hay, log a api_logs, return JSON
```

## 7.4 Probar local

```bash
cd api
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
export POSTGRES_URI="postgresql://postgres:TU_PASSWORD@IP:5432/recos_db"
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Probar:
```
http://localhost:8000/health
http://localhost:8000/recommendations/adv_001/TopCTR
http://localhost:8000/docs   (Swagger interactivo)
```

---

# 8. Docker — empaquetar la API

## 8.1 Dockerfile

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
ENV PORT=8080
EXPOSE 8080
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
```

## 8.2 .dockerignore

```
__pycache__/
*.pyc
*.pyo
venv/
.venv/
.env
.git/
.gitignore
README.md
```

## 8.3 Build local (opcional)

```bash
cd api
docker build -t tp-final-api:local .
docker run --rm -p 8080:8080 \
  -e POSTGRES_URI="postgresql://postgres:TU_PASSWORD@IP:5432/recos_db" \
  tp-final-api:local
```

Si no tenés Docker Desktop, no hay drama: Cloud Build hace el build en la nube.

---

# 9. Cloud Run — desplegar la API en internet

## 9.1 Crear el repositorio en Artifact Registry

```bash
gcloud artifacts repositories create tp-final-repo \
  --repository-format=docker \
  --location=us-central1 \
  --project=tp-final-adtech-493922
```

## 9.2 Build con Cloud Build

```bash
cd ~/tp-final-adtech
gcloud builds submit api/ \
  --tag=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:v1 \
  --project=tp-final-adtech-493922
```

## 9.3 Deploy

```bash
gcloud run deploy fastapi-tp \
  --image=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:v1 \
  --region=us-central1 \
  --project=tp-final-adtech-493922 \
  --allow-unauthenticated \
  --set-env-vars="POSTGRES_URI=postgresql://postgres:TU_PASSWORD@IP:5432/recos_db"
```

Devuelve una URL tipo `https://fastapi-tp-xbo6kajhza-uc.a.run.app`.

## 9.4 Verificar

```bash
curl $URL/health
curl "$URL/recommendations/adv_001/TopCTR"
# Browser: $URL/docs
```

---

# 10. IP estática para Airflow

```bash
gcloud compute addresses create airflow-vm-ip \
  --addresses=IP_ACTUAL_DE_LA_VM \
  --region=us-central1 \
  --project=tp-final-adtech-493922
```

---

# 11. Usuario profesor en Airflow

```bash
~/airflow_venv/bin/airflow users create \
  --username profesor \
  --firstname Profesor --lastname TPFinal \
  --role Viewer \
  --email profesor@tp-adtech.local \
  --password 'profesor'
```

---

# 12. Repositorio Git público

## 12.1 Antes de hacer público — chequeo de secretos

```bash
grep -rE "password|secret|api[_-]?key" --include='*.py' .
grep -rE "postgres(ql)?://[^/]+:[^@]+@" --include='*.py' --include='*.yml' .
git log -p --all | grep -iE "password|secret" | head
```

## 12.2 Hacer público

GitHub → Settings → Danger Zone → Change repository visibility → Make public.

---

# 13. Errores que nos pasaron y cómo los resolvimos

## 13.1 La connection string que la API no entendía

**Síntoma:** después de un deploy, los endpoints empezaron a tirar 500. `/health` respondía OK pero los demás explotaban. Logs en Cloud Run:

```
psycopg2.ProgrammingError: invalid dsn: missing "=" after
"postgresql+psycopg2://..." in connection info string
```

**Causa:** la URI seteada en Cloud Run tenía prefijo `postgresql+psycopg2://` que SQLAlchemy entiende pero psycopg2 directo no.

**Solución:** normalizar la URI en el código:

```python
if POSTGRES_URI.startswith("postgresql+psycopg2://"):
    POSTGRES_URI = POSTGRES_URI.replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )
```

## 13.2 El scheduler de Airflow huérfano sin variables de entorno

**Síntoma:** durante varios días el DAG no procesó datos nuevos. Corriendo `airflow dags list`:

```
Error: Failed to load all files. For details, run
`airflow dags list-import-errors`
No data found
```

`list-import-errors` mostraba: `RuntimeError: Falta la variable de entorno POSTGRES_URI`.

**Causa:** la variable estaba en `.bashrc`, pero `.bashrc` solo se lee en shells interactivas. El scheduler había arrancado con la variable, pero la shell se cerró y el proceso quedó huérfano (PPID=1). Cuando Airflow recicla workers, no heredan más la variable.

**Solución:** matar todos los procesos viejos, abrir shell limpia con `source ~/.bashrc`, volver a arrancar. Mejora pendiente: crear servicios systemd con la variable en el archivo `.service`.

## 13.3 Confusión con el nombre del DAG

**Síntoma:** `airflow dags list-runs -d adtech_recos` devolvía vacío.

**Causa:** en el código el `dag_id` es `adtech_pipeline`, no `adtech_recos`. Habíamos cambiado el nombre.

**Lección:** los nombres son contratos. Si el `dag_id` en el código no matchea con la documentación, todo se rompe.

## 13.4 El DAG está pausado y no procesa

**Síntoma:** el DAG aparece en `airflow dags list` pero no corre runs nuevos.

**Diagnóstico:**

```
adtech_pipeline | /home/.../recomendaciones_dag.py | airflow | True
                                                              ↑
                                                        paused = True
```

**Solución:**

```bash
~/airflow_venv/bin/airflow dags unpause adtech_pipeline
```

## 13.5 Imagen Docker pesa 1GB+

**Causas comunes:**
- Usar `python:3.12` en lugar de `python:3.12-slim`.
- Olvidarse de `.dockerignore` y copiar `venv/` al container.

**Solución:** revisar `.dockerignore` y usar la base slim.

## 13.6 Endpoint /history/ devuelve solo un día

**Síntoma:** la consigna pide que `/history/` devuelva los últimos 7 días, pero solo devuelve uno.

**Causa:** el DAG hace `df.to_sql(..., if_exists="replace")`, lo que borra las tablas en cada run. Nunca hay más de un día en la base.

**Solución (mejora pendiente):** cambiar el `db_writing` para usar `if_exists="append"` precedido de un `DELETE FROM ... WHERE date = %s`. De esta forma se borra solo el día que se está reprocesando y se mantiene el histórico de días anteriores.

---

# 14. Cómo mantener todo vivo entre la entrega y el coloquio

## 14.1 Qué NO tocar entre 9/5 y 13/5

```
✗ NO detener la VM airflow-vm
✗ NO detener la instancia Cloud SQL
✗ NO eliminar el servicio Cloud Run fastapi-tp
✗ NO eliminar imágenes de Artifact Registry
✗ NO revocar el firewall rule allow-airflow-ui
✗ NO borrar el usuario profesor de Airflow
```

## 14.2 Smoke test cada 1-2 días

```bash
URL=https://fastapi-tp-xbo6kajhza-uc.a.run.app
IP=34.55.205.251

curl -s -o /dev/null -w "API: %{http_code}\n" $URL/health
curl -s -o /dev/null -w "Airflow: %{http_code}\n" http://$IP:8080/
```

Esperás `API: 200` y `Airflow: 302`.

## 14.3 Riesgo conocido

El scheduler de Airflow corre con `nohup`, no con systemd. Si la VM se reinicia (mantenimiento de Google), Airflow no se levanta solo. Probabilidad baja en 4 días pero existe. **Mejora pendiente:** crear servicios systemd.

## 14.4 Después del coloquio — limpieza

```bash
gcloud compute instances stop airflow-vm --zone=us-central1-a
gcloud sql instances patch tp-final-adtech-493922 --activation-policy=NEVER
gcloud run services delete fastapi-tp --region=us-central1
```

---

# 15. Glosario

**API REST:** una manera estándar de exponer datos por HTTP. Cada URL representa un recurso, cada método HTTP representa una acción.

**Airflow:** un orquestador de workflows. Vos describís tareas y dependencias, Airflow se encarga de correrlas en orden.

**Artifact Registry:** el "Docker Hub privado" de Google Cloud.

**Bucket:** una carpeta raíz en Cloud Storage.

**Cloud Build:** servicio que construye imágenes Docker en la nube.

**Cloud Run:** plataforma serverless que ejecuta containers Docker detrás de una URL HTTPS. Escala a cero cuando no hay tráfico.

**Cloud SQL:** PostgreSQL administrado por Google.

**Cloud Storage (GCS):** disco gigante en la nube.

**Compute Engine:** máquinas virtuales en la nube.

**Container:** una imagen Docker corriendo. Es un proceso vivo aislado.

**DAG:** Directed Acyclic Graph. En Airflow, un Python que describe tareas y dependencias.

**Docker:** sistema para empaquetar aplicaciones con su entorno completo en imágenes portables.

**Dockerfile:** archivo de texto con instrucciones para construir una imagen Docker.

**ds (date string):** variable que Airflow inyecta en cada tarea con la fecha lógica del run, formato "YYYY-MM-DD".

**Endpoint:** una URL específica de una API que responde a un método HTTP.

**Env var:** una configuración que se le pasa a un proceso por afuera del código.

**FastAPI:** framework de Python para hacer APIs REST con type hints y documentación automática.

**Imagen Docker:** una receta congelada que contiene un sistema operativo mínimo + tu app + sus dependencias.

**IP estática:** una IP que no cambia aunque la VM se reinicie. Hay que reservarla explícitamente.

**Jaccard similarity:** métrica entre 0 y 1 que mide cuánto se parecen dos conjuntos. Fórmula: `|A ∩ B| / |A ∪ B|`.

**psycopg2:** driver de PostgreSQL para Python. Permite ejecutar queries SQL.

**Path parameter:** una parte de la URL que es variable. Ej: en `/users/{id}`, `{id}` es un path param.

**Query parameter:** parámetros que se pasan después de `?` en la URL.

**Rollback:** volver a una versión anterior cuando algo se rompe.

**Scheduler (de Airflow):** el proceso que despierta cada minuto y dispara los runs según el calendario.

**SQLAlchemy:** un toolkit de Python para hablar con bases relacionales. `pandas.to_sql` lo usa por debajo.

**Systemd:** el sistema en Linux que gestiona servicios al boot.

**Twelve-Factor App:** principios para apps modernas (configuración por env vars, etc.).

**uvicorn:** el servidor que ejecuta FastAPI.

**venv:** carpeta aislada donde Python instala librerías solo para un proyecto.

**Webserver (de Airflow):** la UI web de Airflow.

---

**Fin de la guía.**
