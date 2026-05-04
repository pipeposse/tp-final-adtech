# Guía paso a paso — TP Final AdTech

**Programación Avanzada · UDESA · 2026**

Esta guía cuenta cómo armamos el sistema desde cero. Está pensada para que cualquiera del equipo (o cualquier curioso) pueda seguirla, entender qué hace cada pieza, defenderlo en el coloquio, y reproducirlo si hace falta. **No damos nada por sentado.** Si una parte parece obvia, la explicamos igual, porque seguro que para alguien no lo es.

Importante: la guía describe el sistema **tal como está implementado hoy**, con sus decisiones reales y sus limitaciones. Las cosas que decidimos no implementar o que dejamos para una mejora futura aparecen claramente marcadas como tales. Si algo del informe parece inflado, abrí el código del repo y vas a ver que coincide.

---

## Tabla de contenidos

```
0.  Antes de arrancar — concepto general
1.  Crear el proyecto en Google Cloud
2.  Cloud Storage — donde viven los datos crudos
3.  Cloud SQL — la base PostgreSQL
4.  Compute Engine — la VM para Airflow
5.  Instalar y levantar Airflow
6.  Escribir el DAG (el pipeline)
7.  La API con FastAPI
8.  Docker — empaquetar la API
9.  Cloud Run — desplegar la API en internet
10. IP estática para Airflow
11. Usuario profesor en Airflow
12. Repositorio Git público
13. Errores que tuvimos y cómo los resolvimos
14. Cómo mantener todo vivo entre la entrega y el coloquio
15. Glosario — todos los términos en castellano
```

---

# 0. Antes de arrancar

## ¿Qué es este sistema?

Un sistema de recomendación de productos para anunciantes. La idea, en cuatro líneas:

```
Cada día caen archivos con datos de impresiones y clicks
        ↓
Un proceso los toma, los procesa, y calcula los 20 productos
"top" para cada anunciante según dos criterios distintos
        ↓
Esos resultados se guardan en una base de datos
        ↓
Una API web los devuelve cuando alguien la consulta
```

Los archivos viven en **Google Cloud Storage** (GCS, una carpeta gigante en la nube). El procesamiento lo orquesta **Apache Airflow** (que es el "señor que tiene la agenda y dice qué tarea correr cuándo") en una máquina virtual de **Compute Engine**. Los resultados se guardan en una base PostgreSQL administrada por Google (**Cloud SQL**), y la API la sirve **Cloud Run** desde una imagen Docker.

## Las cinco piezas en una foto

```
   ┌─────────────────┐
   │  Cloud Storage  │   ← bucket con CSVs diarios
   └────────┬────────┘
            │  (lo bajamos a mano con gsutil cp)
            ▼
   ┌─────────────────────────────┐
   │   Compute Engine VM          │
   │   (e2-small, Linux)          │
   │   ├── Airflow scheduler      │
   │   ├── Airflow webserver      │
   │   └── DAG adtech_pipeline    │
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
   │   Cloud Run     │   ← FastAPI sirviendo HTTPS
   │   (Docker)      │
   └────────▲────────┘
            │
   Profesor / corrector / cliente HTTP
```

## ¿Qué hay que saber antes de meterse?

Cosas que ASUMIMOS que ya manejamos (al menos un poquito).

- **Linux básico**: moverte por la terminal, `cd`, `ls`, editar archivos con `nano`.
- **Python**: qué es un `venv`, `pip install`, escribir un script.
- **SQL**: `CREATE TABLE`, `INSERT`, `SELECT`. Nada raro.
- **HTTP**: GET, códigos 200/400/404/500, JSON.
- **Git**: clonar, commit, push.
- **Docker conceptual**: imagen vs container. El resto lo explicamos en la sección 8.

## Tip almacenero

No intentear hacer todo de una. Resolvé una fase, dejala andando, y recién pasá a la siguiente. Si arrancás a escribir el DAG antes de tener Cloud SQL listo, vas a sufrir el doble.

---

# 1. Crear el proyecto en Google Cloud

## 1.1 Tener una cuenta

Vamos a https://console.cloud.google.com con tu cuenta de Google.

## 1.2 Crear el proyecto

Arriba a la izquierda, donde dice "Selecciona un proyecto" → **Nuevo proyecto**.

```
Nombre:           tp-final-adtech (o lo que quieras)
ID del proyecto:  lo genera Google solo (algo como tp-final-adtech-493922)
Organización:     "Sin organización" si es cuenta personal
```

**Anotá el Project ID.** Es distinto al nombre, lo vas a usar en TODOS los comandos. En nuestro caso fue `tp-final-adtech-493922`.

## 1.3 Habilitar las APIs que vamos a usar

GCP es como un mercadito: tiene cientos de servicios pero por defecto vienen apagados. Hay que prender los que vamos a usar.

Por consola web: Menú ≡ → **APIs y servicios** → **Biblioteca**. Buscá y habilitá uno por uno:

- Compute Engine API
- Cloud SQL Admin API
- Cloud Storage API (suele estar habilitada por default)
- Cloud Run Admin API
- Artifact Registry API
- Cloud Build API
- IAM Service Account Credentials API

Atajo si tenés `gcloud` instalado en tu compu:

```bash
gcloud services enable \
  compute.googleapis.com sqladmin.googleapis.com \
  storage.googleapis.com run.googleapis.com \
  artifactregistry.googleapis.com cloudbuild.googleapis.com \
  iamcredentials.googleapis.com
```

## 1.4 Una sola región para todo

Mezclar regiones=problemas futuros

Nosotros usamos **`us-central1`** (Iowa). Es de las más baratas y tiene todos los servicios.

> **Tip:** anotate en un papel: Project ID, Región, Project Number. Los vas a usar mil veces. Si no los tenés a mano, vas a tipear mal y vas a pelearte con errores tontos.

---

# 2. Cloud Storage — donde viven los datos crudos

## 2.1 Concepto

GCS es básicamente un **disco gigante en la nube**. Vos creás "buckets" (carpetas raíz) y subís archivos adentro. Pagás por GB guardado y por GB descargado. Es muchísimo más barato que meter todo en una base.

## 2.2 Crear el bucket

Por consola web: Menú ≡ → **Cloud Storage** → **Buckets** → **CREATE**.

```
Nombre:           tp-final-adtech
                  (los nombres son globalmente únicos en todo GCP,
                  conviene ponerle un prefijo único)

Tipo:             Region (no Multi-region, es más caro)
Región:           us-central1
Storage class:    Standard
Access control:   Uniform
Protection:       None (para un TP está bien)
```

## 2.3 Subir los CSVs crudos

El TP usa tres datasets:

```
active_advertisers.csv          (lista de anunciantes activos, no cambia por día)
ads_views_YYYY-MM-DD.csv        (impresiones y clicks, uno por día)
product_views_YYYY-MM-DD.csv    (visualizaciones, uno por día)
```

Subilos desde la consola web (botón **UPLOAD FILES**) o por terminal:

```bash
gsutil cp *.csv gs://tp-final-adtech/raw/
```

Convención de carpetas:

```
gs://tp-final-adtech/
└── raw/
    ├── active_advertisers.csv
    ├── ads_views_2026-04-18.csv
    └── product_views_2026-04-18.csv
    ...
```

> **Detalle:** GCS no tiene "carpetas" de verdad, son prefijos en el nombre. Pero la consola los muestra como carpetas, así que para nuestros fines da igual.

---

# 3. Cloud SQL — la base PostgreSQL

## 3.1 Concepto

Cloud SQL es **PostgreSQL administrado**. En vez de instalar Postgres en una VM y bancarte parches, backups y crashes, Google te lo entrega listo.

## 3.2 Crear la instancia

**Tarda 8-10 minutos.** Lanzala AHORA y mientras seguís con los pasos siguientes.

Menú ≡ → **SQL** → **CREATE INSTANCE** → **PostgreSQL**.

```
Instance ID:        tp-final-adtech-493922
Password postgres:  (algo robusto y ANOTALO)
Database version:   PostgreSQL 15
Región:             us-central1
Zone:               Single zone
Preset:             Sandbox / Development
Machine type:       db-f1-micro o db-g1-small
Storage:            10 GB SSD
```

En **Connections**: marcamos **Public IP**. Authorized networks vacío por ahora; después agregás IPs cuando hagan falta.

Click **CREATE INSTANCE** y a esperar.

## 3.3 Anotar los datos de conexión

Cuando esté lista (ícono verde), dentro de la instancia → Overview:

```
Public IP:        algo como 34.46.239.72
Connection name:  tp-final-adtech-493922:us-central1:tp-final-adtech-493922
                  (formato: PROJECT:REGION:INSTANCE)
```

## 3.4 Crear las dos bases adentro de la instancia

Una sola instancia de Postgres puede contener varias bases lógicas. Vamos a crear dos:

```
airflow_db   ← metadata interna de Airflow (DAGs, runs, logs)
recos_db     ← tablas del proyecto (top_ctr, top_product, api_logs)
```


Por consola: dentro de la instancia → **Databases** → **CREATE DATABASE**. Repetir para la otra.

## 3.5 El esquema (informativo)

**No tenés que crear las tablas a mano.** Se crean solas en runtime:

- `top_ctr` y `top_product` las crea el DAG con `pandas.to_sql`. Como el DAG usa `if_exists="replace"`, en cada corrida se borran y reescriben enteras. Importante saber esto: la base nunca tiene más de un día de datos. Lo tratamos como limitación en la sección 13.

- `api_logs` la crea la API al arrancar (en el handler `lifespan`).

Si querés ver el esquema descriptivo, está en `sql/schema.sql` del repo.

## 3.6 Conectarte por primera vez

Desde Cloud Shell (botón `>_` arriba a la derecha en la consola):

```bash
gcloud sql connect tp-final-adtech-493922 --user=postgres --database=recos_db
```

Te pide la password de postgres (la que anotaste). Si entrás, ves un prompt `recos_db=>`. Tipeá `\dt` para listar tablas (la primera vez está vacío) y `\q` para salir.

> **Si te da timeout**: tenés que agregar tu IP a "Authorized networks". En la consola → Connections → Networking → ADD NETWORK → tu IP pública (la sacás en https://www.whatismyip.com).

---

# 4. Compute Engine — la VM para Airflow

## 4.1 ¿Por qué una VM y no Cloud Run?

Cloud Run apaga el container cuando no hay tráfico, lo cual está bárbaro para una API pero **mata a Airflow**. El scheduler de Airflow tiene que estar siempre prendido revisando si toca disparar algún DAG. Por eso necesitamos una VM tradicional.

## 4.2 Crear la VM

Menú ≡ → **Compute Engine** → **VM instances** → **CREATE INSTANCE**.

```
Name:               airflow-vm
Region/Zone:        us-central1 / us-central1-a
Machine type:       E2 → e2-small  (obligatorio por la consigna)
Boot disk:          Ubuntu 22.04 LTS, 20 GB
Firewall:           ✓ Allow HTTP, ✓ Allow HTTPS
Service account:    default con "Allow full access to all Cloud APIs"
```

## 4.3 Abrir el puerto 8080 (la UI de Airflow)

Por defecto, las VMs solo aceptan SSH (puerto 22). Airflow corre su web en 8080, así que hay que abrirlo en el firewall.

Menú ≡ → **VPC network** → **Firewall** → **CREATE FIREWALL RULE**:

```
Name:               allow-airflow-ui
Direction:          Ingress
Action:             Allow
Targets:            All instances in the network
Source IPv4 ranges: 0.0.0.0/0
                    (cualquier IP - aceptable porque Airflow pide login)
Protocols/ports:    TCP 8080
```

## 4.4 Entrar por SSH

En la lista de VMs, botón **SSH** al lado de `airflow-vm`. Se abre una terminal en el navegador. No hace falta nada más.

Alternativa por terminal local:

```bash
gcloud compute ssh airflow-vm --zone=us-central1-a --project=tp-final-adtech-493922
```

---

# 5. Instalar y levantar Airflow

Todos los comandos de esta sección se ejecutan adentro de la VM por SSH.

## 5.1 Actualizar el sistema e instalar paquetes base

```bash
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv libpq-dev postgresql-client git
```

**Por qué cada uno:**
- `python3-pip`, `python3-venv`: Python ya viene en Ubuntu, pero estos hay que instalarlos.
- `libpq-dev`: headers de Postgres. Sin esto, `pip install psycopg2` falla.
- `postgresql-client`: el comando `psql` para probar la conexión.
- `git`: para clonar el repo a la VM.

## 5.2 Verificar que la VM llega a la base

```bash
psql "host=34.46.239.72 port=5432 user=postgres dbname=airflow_db sslmode=require"
```

Reemplazá la IP por la de tu instancia. Te pide password (la de postgres) y deberías ver el prompt `airflow_db=>`. Tipeá `\q` para salir.

> **Si da timeout**: agregá la IP externa de la VM (la ves en la consola de Compute Engine) a las Authorized networks de Cloud SQL.

## 5.3 Crear un venv y configurar AIRFLOW_HOME

```bash
cd ~
python3 -m venv airflow_venv
source airflow_venv/bin/activate
```

**Concepto de venv:** un entorno virtual es un directorio aislado donde pip instala librerías solo para ese proyecto. Evita que las dependencias de Airflow choquen con las del sistema u otros proyectos. Cuando hacés `source ... activate`, el prompt cambia y muestra `(airflow_venv)`. Estás "adentro" del venv: cualquier `pip install` instala ahí.

Configurar AIRFLOW_HOME (la carpeta donde Airflow guarda config, DAGs, logs):

```bash
export AIRFLOW_HOME=~/airflow
echo 'export AIRFLOW_HOME=~/airflow' >> ~/.bashrc
echo 'source ~/airflow_venv/bin/activate' >> ~/.bashrc
```

Las dos últimas líneas hacen que cada vez que abras una terminal, automáticamente se active el venv y se setee AIRFLOW_HOME.

## 5.4 Setear POSTGRES_URI

Esta variable la van a leer Airflow y nuestro DAG.

```bash
echo 'export POSTGRES_URI="postgresql://postgres:TU_PASSWORD@34.46.239.72:5432/recos_db"' >> ~/.bashrc
source ~/.bashrc
```

> **Cuidado con caracteres especiales en la password.** Si tu password tiene `@`, `:`, `#`, `;`, etc., tenés que **URL-encodearla**. Ejemplo: `@` → `%40`, `#` → `%23`. Hay sitios "URL encoder" online que te lo hacen.

## 5.5 Instalar Airflow con el constraints file

Airflow es quisquilloso con las versiones. Nunca lo instales a secas, **siempre** usá el constraints file oficial:

```bash
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION="$(python --version | cut -d ' ' -f2 | cut -d '.' -f1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" \
  "apache-airflow-providers-postgres" \
  --constraint "${CONSTRAINT_URL}"

pip install pandas psycopg2-binary sqlalchemy
```

> **Nota:** SQLAlchemy se instala porque el DAG la necesita. La usa indirectamente a través de `pandas.to_sql`. Más detalle en la sección 6.

## 5.6 Configurar airflow.cfg

Por defecto Airflow usa SQLite (un archivo en disco). Para producción usamos Postgres. Editá el config:

```bash
nano $AIRFLOW_HOME/airflow.cfg
```

Buscá y cambiá estas líneas:

```ini
[core]
executor = LocalExecutor
parallelism = 2
load_examples = False

[database]
sql_alchemy_conn = postgresql://postgres:TU_PASSWORD@34.46.239.72:5432/airflow_db

[webserver]
workers = 1
```

Guardar con `Ctrl+O`, Enter, salir con `Ctrl+X`.

## 5.7 Inicializar la base de Airflow y crear el admin

```bash
airflow db migrate

airflow users create \
  --username admin \
  --firstname Pipe --lastname Posse \
  --role Admin --email tu@email.com \
  --password admin123
```

`airflow db migrate` crea todas las tablas internas de Airflow en `airflow_db`. La primera vez tarda 30-60 segundos.

## 5.8 Levantar webserver y scheduler

```bash
airflow webserver -p 8080 -D
airflow scheduler -D
```

El `-D` es "daemon": los procesos quedan corriendo en background y sobreviven al cierre de la terminal SSH (con limitaciones: si la VM se reinicia, no se levantan solos).

> **Mejora pendiente reconocida:** lo prolijo es crear servicios systemd para que Airflow se reinicie solo al boot de la VM. Por tiempo lo dejamos con `nohup` / `-D`. Lo documentamos en la sección 13.

## 5.9 Verificar

Abrí en el navegador:

```
http://IP_EXTERNA_VM:8080
```

Tendría que aparecer la pantalla de login de Airflow. Entrá con `admin / admin123`. Vas a ver la lista de DAGs (vacía por ahora).

---

# 6. Escribir el DAG (el pipeline)

## 6.1 ¿Qué es un DAG?

DAG = "Directed Acyclic Graph" = grafo dirigido sin ciclos. En Airflow, **un DAG es un Python que define un conjunto de tareas y sus dependencias**.

Vos describís: "tarea A primero, después B y C en paralelo, después D al final". Airflow se encarga del orden, los reintentos, los logs, y la UI.

## 6.2 Estructura del DAG nuestro

```
   ┌──────────────┐
   │ FiltrarDatos │   1. Lee CSVs locales del día, filtra
   └──────┬───────┘      por active_advertisers, guarda en /tmp
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

## 6.3 El código real del DAG

Crear el archivo en la VM:

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
- `ds` es la fecha lógica del run (date string), formato `"YYYY-MM-DD"`. La inyecta Airflow automáticamente.
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

> **Limitación reconocida:** el `if r["impressions_count"] > 0 else 0` evita división por cero, pero **NO** descarta productos con pocas impresiones. Un producto con 2 impresiones y 2 clicks tiene CTR=1.0 y puede ganar el ranking. Una mejora directa sería agregar `impressions_count >= N` antes de calcular el ranking. Lo dejamos como mejora pendiente.

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
- `create_engine(POSTGRES_URI)` crea un engine de SQLAlchemy. `pandas.to_sql` lo necesita: pandas no habla con Postgres directo, delega en SQLAlchemy.
- `if_exists="replace"` significa: si la tabla existe, **borrarla y recrearla**. Cada run del DAG **borra y reescribe enteras** las dos tablas.

> **Limitación importante:** con `if_exists="replace"`, la base nunca tiene más de un día de datos. Esto rompe la promesa del endpoint `/history/{advertiser_id}/` (los últimos 7 días): en la práctica, devuelve solo el último día. La mejora directa sería usar `if_exists="append"` después de un `DELETE FROM ... WHERE date = ds`, manteniendo histórico real.

> **Nota sobre asimetría:** la API se conecta a Postgres con psycopg2 directo (sin ORM), pero el DAG usa SQLAlchemy a través de pandas. Es una asimetría heredada de cómo arrancamos el proyecto. Una mejora prolija sería unificar ambos componentes en psycopg2 directo.

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
- `dag_id="adtech_pipeline"`: nombre interno único del DAG.
- `schedule="@daily"`: el scheduler dispara un run por día (00:00 UTC).
- `catchup=False`: no procesar runs viejos cuando se prende. Si lo activás un lunes, no procesa todos los días desde `start_date`.
- `t1 >> [t2, t3] >> t4`: dependencias. t1 primero, t2 y t3 en paralelo, t4 al final.

## 6.4 Bajar los CSVs del bucket a la VM

```bash
mkdir -p ~/trabajo_practico
gsutil -m cp gs://tp-final-adtech/raw/*.csv ~/trabajo_practico/
```

## 6.5 Probar el DAG manualmente

Antes de prender el schedule, probá cada tarea:

```bash
airflow tasks test adtech_pipeline FiltrarDatos 2026-04-18
airflow tasks test adtech_pipeline top_ctr 2026-04-18
airflow tasks test adtech_pipeline top_product 2026-04-18
airflow tasks test adtech_pipeline DBWriting 2026-04-18
```

Si las cuatro terminan con `Marking task as SUCCESS`, todo bien. En la UI de Airflow, prendé el toggle del DAG y va a empezar a correr cada día.

---

# 7. La API con FastAPI

## 7.1 Estructura del proyecto

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

## 7.3 main.py — los puntos clave del código

El código completo está en el repo (`api/app/main.py`). Acá te muestro los pedazos importantes con explicación.

### Normalización de la URI

```python
POSTGRES_URI = os.getenv("POSTGRES_URI")
if not POSTGRES_URI:
    raise RuntimeError("Falta la variable de entorno POSTGRES_URI")

# psycopg2 directo no entiende el dialecto SQLAlchemy.
# Lo normalizamos al formato estándar.
if POSTGRES_URI.startswith("postgresql+psycopg2://"):
    POSTGRES_URI = POSTGRES_URI.replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )
```

> Esta normalización la agregamos cuando descubrimos que la URI seteada en Cloud Run venía con prefijo extendido. Detalle completo en la sección 13.

### Conexión a la base con context manager

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

Esto es un **context manager**. La idea: cada vez que necesitás hablar con la base, abrís una conexión, hacés tu cosa, cerrás. Si algo falla, hay que cerrar igual. Python tiene `with` para esto.

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

**Notar:**
- `@app.get(...)` es un decorator. Le dice a FastAPI que esta función responde a GET en esa URL.
- Las `{advertiser_id}/{modelo}` capturan partes de la URL como argumentos.
- Si el modelo no existe, devolvemos 400.
- Si no hay datos, 404.
- `RealDictCursor` hace que las filas vuelvan como dicts (`row["advertiser_id"]`) en vez de tuplas.

## 7.4 Probar local

```bash
cd api
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
export POSTGRES_URI="postgresql://postgres:TU_PASSWORD@IP:5432/recos_db"
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Probar en otra terminal:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/recommendations/LW045DVYSGRD75TK6U54/TopCTR
```

**Bonus:** abrí en el browser `http://localhost:8000/docs`. FastAPI te genera Swagger UI gratis. Mostrale esto al corrector en el coloquio, queda muy bien.

---

# 8. Docker — empaquetar la API

## 8.1 Concepto

Docker resuelve el problema de "en mi máquina anda pero en producción no". Empaquetás la app **junto con su sistema operativo y todas sus dependencias** en un bloque inmutable que corre igual en cualquier lado.

Tres palabras clave:
- **Imagen**: una receta congelada. Pesa megas.
- **Container**: la imagen corriendo. Es un proceso vivo.
- **Dockerfile**: el archivo de texto con las instrucciones para construir la imagen.

## 8.2 El Dockerfile

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

**Línea por línea:**
- `FROM python:3.12-slim`: imagen base con Linux Debian + Python 3.12. La "slim" pesa ~50MB en vez de ~900MB.
- `WORKDIR /app`: dentro del container, posicionate en `/app`.
- `COPY requirements.txt .`: copia el archivo de requirements.
- `RUN pip install ...`: instala las dependencias.
- `COPY app/ ./app/`: copia el código.
- `ENV PORT=8080`: define una env var.
- `EXPOSE 8080`: documenta que el container escucha en 8080.
- `CMD [...]`: el comando que arranca el container.

**Por qué `--host 0.0.0.0`:** en un container, si dejás `127.0.0.1` (localhost), el container solo se habla a sí mismo. `0.0.0.0` significa "escuchá en todas las interfaces". Es necesario para que Cloud Run pueda hablarle.

**Por qué este orden de instrucciones:** Docker arma capas y cachea. Si copiás `requirements.txt` antes que el código, cuando solo cambia el código no reinstala las dependencias. Si lo hacés al revés, cada cambio en código reinstala todo. Buena práctica.

## 8.3 .dockerignore

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

Excluye archivos del build context. **Especialmente importante:** `.env` (no metas secretos en la imagen) y `venv/` (pesado e inútil dentro del container).

## 8.4 Build local (opcional)

Si tenés Docker Desktop:

```bash
cd api
docker build -t tp-final-api:local .
docker run --rm -p 8080:8080 \
  -e POSTGRES_URI="postgresql://postgres:TU_PASSWORD@IP:5432/recos_db" \
  tp-final-api:local
```

Si NO tenés Docker local, no problem: en el siguiente paso, Cloud Build lo hace en la nube.

---

# 9. Cloud Run — desplegar la API en internet

## 9.1 Concepto

Cloud Run ejecuta containers Docker detrás de una URL HTTPS. Es **serverless**: si nadie consulta, escala a cero containers (no pagás). Si llegan 1000 requests, levanta 50 containers en paralelo.

## 9.2 Crear el repositorio en Artifact Registry

Artifact Registry es donde se guardan las imágenes Docker. Crear el repo una sola vez:

```bash
gcloud artifacts repositories create tp-final-repo \
  --repository-format=docker \
  --location=us-central1 \
  --project=tp-final-adtech-493922
```

## 9.3 Build de la imagen con Cloud Build

Esto sube la carpeta `api/` a la nube, hace el build allá, y deja la imagen en Artifact Registry. **No necesitás Docker local.**

```bash
cd ~/tp-final-adtech
gcloud builds submit api/ \
  --tag=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:v1 \
  --project=tp-final-adtech-493922
```

Tarda 1-2 minutos. Esperás a que diga `STATUS: SUCCESS`.

**El tag `:v1` es importante.** Cada vez que cambies el código y rebuildees, incrementá el número (`:v2`, `:v3`, etc.). Esto te permite hacer rollback si algo se rompe — la imagen anterior sigue en el registry.

## 9.4 Deploy a Cloud Run

```bash
gcloud run deploy fastapi-tp \
  --image=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:v1 \
  --region=us-central1 \
  --project=tp-final-adtech-493922 \
  --allow-unauthenticated \
  --set-env-vars="POSTGRES_URI=postgresql://postgres:TU_PASSWORD@IP:5432/recos_db"
```

**Línea por línea:**
- `--image`: qué imagen usar.
- `--region`: misma región que todo.
- `--allow-unauthenticated`: cualquiera con la URL puede entrar (es API pública).
- `--set-env-vars`: la URI de Postgres como env var. **Nunca commitees esto al repo.**

Al terminar te devuelve una URL tipo `https://fastapi-tp-xbo6kajhza-uc.a.run.app`.

## 9.5 Verificar

```bash
URL=https://fastapi-tp-xbo6kajhza-uc.a.run.app
curl $URL/health
curl "$URL/recommendations/LW045DVYSGRD75TK6U54/TopCTR"
```

Y abrí en el browser `$URL/docs` para ver el Swagger interactivo. **Esto es lo que el corrector va a usar.**

---

# 10. IP estática para Airflow

## 10.1 Por qué importa

La IP externa de tu VM es **efímera por defecto**: si la VM se reinicia, te toca otra. Eso significa que la URL `http://IP/` que le diste al corrector puede dejar de funcionar. Solución: reservar la IP como **estática**, así queda fija para siempre.

## 10.2 Cómo

Por consola: Menú ≡ → **VPC network** → **IP addresses** → buscás la IP de tu VM → **Reserve static address** → ponés un nombre.

O por CLI:

```bash
gcloud compute addresses create airflow-vm-ip \
  --addresses=IP_ACTUAL_DE_LA_VM \
  --region=us-central1 \
  --project=tp-final-adtech-493922
```

> Mientras la IP esté **asignada a una VM**, es gratis. Si la dejás reservada sin usar, te cobran ~3 USD/mes.

---

# 11. Usuario profesor en Airflow

Para que el corrector pueda entrar a la UI de Airflow sin riesgo de tocar nada, le creamos un usuario con rol Viewer (solo lectura):

```bash
~/airflow_venv/bin/airflow users create \
  --username profesor \
  --firstname Profesor --lastname TPFinal \
  --role Viewer \
  --email profesor@tp-adtech.local \
  --password 'profesor'
```

Le mandás al corrector:

```
URL:      http://IP_ESTATICA:8080
Usuario:  profesor
Password: profesor
```

> **Roles de Airflow:**
> - **Admin**: todo.
> - **Op**: triggerear runs, editar conexiones.
> - **User**: triggerear runs.
> - **Viewer**: solo ver. ← **Recomendado para el corrector.**
> - **Public**: sin login, bloqueado.

---

# 12. Repositorio Git público

## 12.1 Estructura del repo

```
tp-final-adtech/
├── airflow_pipeline/
│   ├── dags/
│   │   └── recomendaciones_dag.py
│   └── requirements.txt
├── api/
│   ├── app/
│   │   ├── __init__.py
│   │   └── main.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── .dockerignore
├── docs/
│   ├── informe.md
│   └── guia_paso_a_paso.md
├── sql/
│   └── schema.sql
├── .gitignore
└── README.md
```

## 12.2 Antes de hacer público — chequear que no haya secretos

Esto es **crítico**. Si subís contraseñas a un repo público, hay bots que las detectan en minutos y las usan. Tirá:

```bash
grep -rE "password|secret|api[_-]?key" --include='*.py' .
grep -rE "postgres(ql)?://[^/]+:[^@]+@" --include='*.py' --include='*.yml' .
git log -p --all | grep -iE "password|secret" | head
```

Si aparece alguna credencial **real** (no un placeholder tipo `TU_PASSWORD`), hay que limpiarla antes de hacer público.

## 12.3 .env.example

```
# Copiar como .env y reemplazar con valores reales
POSTGRES_URI=postgresql://postgres:TU_PASSWORD@HOST:PORT/recos_db
```

## 12.4 Hacer el repo público

En GitHub:
1. https://github.com/TU_USUARIO/tp-final-adtech → **Settings**.
2. Scroll hasta el final → sección **Danger Zone**.
3. **Change repository visibility** → **Make public**.
4. Te pide tipear el nombre del repo para confirmar.

## 12.5 README

Es lo primero que ve el corrector. Ya está armado en el repo. Tiene: descripción, arquitectura, URLs públicas, credenciales, stack técnico, estructura, despliegue, limitaciones.

---

# 13. Errores que tuvimos y cómo los resolvimos

## 13.1 La connection string que la API no entendía

**Síntoma:** después de un deploy, los endpoints empezaron a tirar 500 Internal Server Error. `/health` respondía OK pero los demás explotaban.

**Logs en Cloud Run:**

```
psycopg2.ProgrammingError: invalid dsn: missing "=" after
"postgresql+psycopg2://..." in connection info string
```

**Causa:** la URI seteada en Cloud Run tenía prefijo `postgresql+psycopg2://` que SQLAlchemy entiende pero psycopg2 directo no.

**Solución:** normalizar la URI en el código.

```python
if POSTGRES_URI.startswith("postgresql+psycopg2://"):
    POSTGRES_URI = POSTGRES_URI.replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )
```

## 13.2 El scheduler de Airflow huérfano sin variables de entorno

**Síntoma:** durante varios días el DAG no procesó datos nuevos. La fecha en la API se quedó pegada.

**Diagnóstico:** corriendo `airflow dags list`:

```
Error: Failed to load all files. For details, run
`airflow dags list-import-errors`
No data found
```

`list-import-errors` mostraba:

```
RuntimeError: Falta la variable de entorno POSTGRES_URI.
```

**Causa:** la variable estaba en `.bashrc`, pero `.bashrc` solo se lee en shells interactivas. El scheduler había arrancado con la variable, pero la shell se cerró y el proceso quedó **huérfano** (PPID=1, adoptado por init). Cuando Airflow recicla workers, no heredan más la variable de la shell original.

**Solución:** matar todos los procesos viejos, abrir shell limpia con `source ~/.bashrc`, volver a arrancar:

```bash
pkill -f "airflow scheduler"
pkill -f "airflow webserver"
sleep 3
source ~/.bashrc
nohup ~/airflow_venv/bin/airflow scheduler > ~/airflow_scheduler.log 2>&1 &
nohup ~/airflow_venv/bin/airflow webserver --port 8080 > ~/airflow_webserver.log 2>&1 &
```

**Mejora pendiente:** crear servicios systemd con la variable en el archivo `.service`.

## 13.3 Confusión con el nombre del DAG

**Síntoma:** `airflow dags list-runs -d adtech_recos` devolvía vacío.

**Causa:** en el código, el `dag_id` es `adtech_pipeline`, no `adtech_recos`. Habíamos cambiado el nombre.

**Lección:** los nombres son contratos. Si el `dag_id` en el código no matchea con la documentación, todo se rompe. Siempre revisar el código fuente como verdad.

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

**Estado:** identificado pero NO resuelto. Lo dejamos como mejora pendiente y lo documentamos en el informe (sección 9). La solución sería cambiar el `db_writing` para usar `if_exists="append"` precedido de un `DELETE FROM ... WHERE date = %s`. De esta forma se borra solo el día que se está reprocesando y se mantiene el histórico.

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

El scheduler de Airflow corre con `nohup`, no con systemd. Si la VM se reinicia (mantenimiento de Google), Airflow no se levanta solo. Probabilidad baja en 4 días pero existe.

## 14.4 Después del coloquio — limpieza

Para no consumir créditos eternamente:

```bash
gcloud compute instances stop airflow-vm --zone=us-central1-a
gcloud sql instances patch tp-final-adtech-493922 --activation-policy=NEVER
gcloud run services delete fastapi-tp --region=us-central1
```

---

# 15. Glosario

**API REST:** una manera estándar de exponer datos por HTTP. Cada URL representa un recurso, cada método HTTP (GET, POST, etc.) representa una acción.

**Airflow:** un orquestador de workflows. Vos describís tareas y dependencias, Airflow se encarga de correrlas en orden, reintentar las que fallan, y mostrarte una UI con qué se rompió.

**Artifact Registry:** el "Docker Hub privado" de Google Cloud.

**Bucket:** una carpeta raíz en Cloud Storage donde subís archivos.

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

**Imagen Docker:** una "receta congelada" que contiene un sistema operativo mínimo + tu app + sus dependencias.

**IP estática:** una IP que no cambia aunque la VM se reinicie.

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

Si en algún punto te perdés siguiendo esto, releé la sección 0 (la foto general) para reorientarte. Y si nunca antes hiciste algo similar y estás con miedo, recordá: nosotros tampoco, y se llegó. Los errores son el camino, no el desvío.
