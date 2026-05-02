# TP Final — Pipeline de Recomendaciones AdTech

**Universidad de San Andrés — Programación Avanzada — 2026**

**Autores:** Felipe Posse, Diego Sanguinetti, Belén Candela Lozada Montanari
**Profesores:** Agustín Mosteiro, Matías Dinota
**Fecha de entrega:** 9 de mayo de 2026

---

## Índice

1. [Resumen ejecutivo](#1-resumen-ejecutivo)
2. [Introducción y objetivos](#2-introducción-y-objetivos)
3. [Arquitectura general](#3-arquitectura-general)
4. [Componentes](#4-componentes)
5. [Modelos de recomendación](#5-modelos-de-recomendación)
6. [Despliegue e infraestructura](#6-despliegue-e-infraestructura)
7. [Decisiones técnicas que tomamos en el camino](#7-decisiones-técnicas-que-tomamos-en-el-camino)
8. [Lo que se nos rompió y cómo lo resolvimos](#8-lo-que-se-nos-rompió-y-cómo-lo-resolvimos)
9. [Limitaciones y mejoras pendientes](#9-limitaciones-y-mejoras-pendientes)
10. [Conclusiones](#10-conclusiones)
11. [Anexos](#11-anexos)

---

## 1. Resumen ejecutivo

En este trabajo armamos un sistema de recomendación de productos para advertisers (anunciantes) usando datos de impresiones y clicks. El sistema procesa archivos diarios que viven en Google Cloud Storage, los transforma con un pipeline orquestado por Apache Airflow, guarda los resultados en una base PostgreSQL en Cloud SQL, y los expone con una API REST hecha en FastAPI y desplegada en Cloud Run.

Implementamos dos modelos: **TopCTR**, que ordena productos por click-through ratio, y **TopProduct**, que los ordena por cantidad de visualizaciones. La API tiene endpoints para consultar la última recomendación, ver el historial disponible, y obtener estadísticas agregadas. Dentro de las estadísticas calculamos la **similaridad de Jaccard** entre los dos modelos para ver qué tan parecidas o distintas son sus recomendaciones, lo cual nos permite justificar la decisión de mantener ambos.

Toda la infraestructura está en Google Cloud Platform, administrada con el SDK gcloud. El servicio FastAPI corre como container Docker en Cloud Run, con imágenes versionadas en Artifact Registry para poder volver atrás si algo se rompe. En este informe documentamos los pasos para reproducirlo y, en una sección dedicada, los problemas que se nos presentaron en el camino y cómo los fuimos resolviendo.

---

## 2. Introducción y objetivos

### 2.1 De qué va el problema

La industria AdTech, que es la publicidad digital programática, mueve diariamente cantidades enormes de eventos. Cada vez que alguien ve un anuncio se registra una impresión, y si hace click, se registra un click. Los anunciantes necesitan información digerida sobre qué productos están funcionando bien para decidir dónde poner la plata, qué creatividades empujar, y a quién apuntar.

Construir este tipo de pipeline implica varios temas que normalmente se enseñan por separado: ingesta de archivos, procesamiento batch, almacenamiento estructurado, y exposición vía API. Lo lindo del trabajo es que integra todo eso en un solo proyecto. Lo desafiante es que cada componente se puede romper de formas distintas, y cuando algo falla en un sistema con cinco piezas, hay que saber dónde mirar.

### 2.2 Qué nos propusimos

- Diseñar y desplegar un pipeline de datos completo en GCP, desde el archivo crudo hasta la API pública.
- Orquestar el procesamiento diario con Apache Airflow, de manera que cada paso quede registrado y se pueda reintentar si falla.
- Implementar los dos modelos pedidos (TopCTR y TopProduct) y agregar un análisis de similaridad para entender cuánto se parecen.
- Construir una API REST en FastAPI que cumpla los endpoints de la consigna, valide bien los inputs y use códigos HTTP correctamente.
- Empaquetar el servicio en Docker y desplegarlo en Cloud Run con versionado, para poder hacer rollback si una versión nueva trae problemas.

### 2.3 Hasta dónde llegamos

El sistema procesa tres datasets de entrada por día (`active_advertisers`, `ads_views` con impresiones y clicks, y `product_views` con visualizaciones de productos). La salida son dos tablas, `top_ctr` y `top_product`, con las veinte recomendaciones por advertiser y por modelo. La API expone consultas individuales, históricas y de estadísticas.

Hay decisiones de diseño que dejamos pendientes y que documentamos en la sección 9: la persistencia de histórico en las tablas, el filtrado por umbral mínimo de impresiones en TopCTR, tests automatizados, y un servicio systemd para que Airflow se reinicie solo. Todas son mejoras conocidas que decidimos posponer para entregar a tiempo.

---

## 3. Arquitectura general

### 3.1 La foto del sistema

Decidimos separar el sistema en cuatro capas con responsabilidades bien diferenciadas. Esto nos permite tocar una capa sin que se rompa el resto, y también facilita explicarlo: cuando algo no anda, sabemos en qué pieza buscar.

```
GCS bucket  (CSVs crudos diarios, en gs://tp-final-adtech/raw/)
     |
     v
Compute Engine VM (e2-small, us-central1-a)
  |-- Apache Airflow (LocalExecutor)
  |   |-- scheduler
  |   |-- webserver (puerto 8080)
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
Cloud Run (FastAPI, imagen Docker fastapi-tp:vN)
     |
     v
Cliente HTTP (corrector / nosotros / cualquiera con la URL)
```

### 3.2 El recorrido del dato, de punta a punta

Los CSVs crudos viven en el bucket `gs://tp-final-adtech/raw/`. Para que el DAG los procese, los bajamos manualmente a la VM con `gsutil cp` y los dejamos en `/home/pipeposse/trabajo_practico/`. Esto es importante: **el DAG no descarga directamente de GCS**, lee archivos locales del filesystem de la VM. Es una limitación reconocida que documentamos en la sección 9.

Cada día a las 00:00 UTC, el scheduler de Airflow dispara el DAG `adtech_pipeline` con el `ds` (date string) correspondiente. Las cuatro tareas se ejecutan en este orden:

1. **FiltrarDatos** lee los CSVs del día y se queda solo con los advertisers que aparecen en `active_advertisers.csv`. Guarda los filtrados en `/tmp`.
2. **top_ctr** y **top_product** corren en paralelo, cada uno lee los archivos filtrados y calcula su ranking de top 20 productos por advertiser.
3. **DBWriting** lee los dos CSVs intermedios y los escribe a Postgres. **Aquí el DAG usa `pandas.to_sql(..., if_exists="replace")`, lo que significa que las tablas `top_ctr` y `top_product` se borran y reescriben enteras en cada corrida del DAG.** No hay acumulación histórica: la base solo guarda el resultado del último run.

Por otro lado, totalmente desacoplado, el servicio FastAPI en Cloud Run consulta esas tablas cuando recibe un request. Cada vez que devuelve una recomendación, registra la consulta en `api_logs`. Esa tabla la usamos en el endpoint de estadísticas para mostrar uso de la API.

Esta separación entre el batch (que procesa una vez al día) y el servicio (que responde 24x7) es muy típica en sistemas reales. Permite escalar y deployar cada uno independientemente.

---

## 4. Componentes

### 4.1 Almacenamiento crudo: Google Cloud Storage

Los CSVs crudos viven en un bucket de Google Cloud Storage. La carpeta `raw/` tiene archivos por fecha, y dentro de cada archivo hay miles de eventos. Los archivos siguen el patrón:

```
active_advertisers.csv          (lista de anunciantes activos, no particionado)
ads_views_YYYY-MM-DD.csv        (impresiones y clicks del día)
product_views_YYYY-MM-DD.csv    (visualizaciones de productos del día)
```

Elegimos GCS por sobre meter todo en Cloud SQL porque para datos no estructurados es mucho más barato y escala mejor. Además queda desacoplado: si nos equivocamos en el procesamiento, podemos reprocesar sin tocar la base.

### 4.2 Orquestación: Apache Airflow

Airflow corre sobre una máquina virtual `airflow-vm` (tipo e2-small) en `us-central1-a` de Compute Engine. Acá tuvimos una decisión importante: GCP ofrece una versión administrada de Airflow que se llama Cloud Composer, que sería lo más prolijo, pero tiene un costo fijo mensual bastante alto. Para un TP no nos cerraban los números, así que fuimos por la VM con Airflow instalado a mano.

El precio de esta decisión es que tenemos que mantener nosotros la VM. Si se reinicia, hay que volver a levantar el scheduler. Para un proyecto en producción real, Composer probablemente justificaría su costo en horas ahorradas; para nosotros no.

La instalación es directa. Hay un entorno virtual de Python en `/home/pipeposse/airflow_venv` con Airflow y todas sus dependencias. La metadata interna (estado de DAGs, runs, logs, conexiones) la guarda en una base llamada `airflow_db` dentro del mismo Cloud SQL del proyecto.

El DAG principal se llama `adtech_pipeline` (no confundir con `adtech_recos`, que era un nombre interno que descartamos durante el desarrollo). Tiene cuatro tareas con dependencias así:

```
FiltrarDatos --> [top_ctr, top_product] --> DBWriting
```

La fecha que procesa cada run viene en el contexto de Airflow como `ds` (date string), y el código usa esa fecha para construir el nombre de los archivos a leer (`ads_views_{ds}.csv`).

La interfaz web está en el puerto 8080 de la VM. Para que el corrector pueda entrar, creamos un usuario aparte con rol Viewer (solo lectura), así puede ver el DAG y los runs pero no toca nada.

### 4.3 Base de datos: Cloud SQL (PostgreSQL 15)

Usamos Cloud SQL como solución de base relacional administrada. La instancia tiene dos bases lógicas separadas: `airflow_db` con la metadata de Airflow, y `recos_db` con los datos del proyecto.

Dentro de `recos_db` tenemos tres tablas:

```
top_ctr     (advertiser_id TEXT, top_products TEXT, date TEXT)
top_product (advertiser_id TEXT, top_products TEXT, date TEXT)
api_logs    (id SERIAL, advertiser_id TEXT, model TEXT,
             date TEXT, timestamp TIMESTAMP DEFAULT NOW())
```

La columna `top_products` guarda la lista de los veinte productos como un literal de Python serializado a texto, con el formato `"['p1', 'p2', ...]"`. Esto fue una decisión heredada de cómo arrancamos el código: el DAG genera la lista con pandas, la serializa a CSV y la escribe a la base con `to_sql`. La API después usa `ast.literal_eval` para deserializar y devolver un array JSON limpio.

**Las tablas `top_ctr` y `top_product` se reescriben enteras en cada run del DAG** (porque el DAG usa `if_exists="replace"`). Esto significa que la base nunca tiene más de un día de datos. Esta es una limitación importante que afecta al endpoint `/history/`, descrita en la sección 9.

La tabla `api_logs` la crea la API automáticamente al arrancar (en el handler de `lifespan`). Registra cada consulta exitosa de recomendaciones, lo cual alimenta el endpoint `/stats/`.

### 4.4 API REST: FastAPI sobre Cloud Run

La API la armamos con FastAPI 0.115, sirviendo a través de Uvicorn en su versión "standard". **La API se conecta a Postgres con psycopg2-binary directo, sin capas intermedias de abstracción.** Las queries son simples (tres SELECTs y un INSERT) y queríamos que el código sea fácil de leer y de defender.

Vale aclarar que **el DAG sí usa SQLAlchemy** indirectamente, a través de `pandas.to_sql`, que requiere un engine de SQLAlchemy. Es una asimetría heredada de cómo arrancamos el proyecto. Una mejora pendiente sería unificar ambos componentes en psycopg2 directo.

El servicio se empaqueta como imagen Docker basada en `python:3.12-slim`. Cloud Build se encarga de hacer el build y subir la imagen a Artifact Registry con un tag versionado (v1, v2, v3, v4). Cloud Run consume esa imagen y la sirve detrás de una URL HTTPS estable, escalando automáticamente según el tráfico.

Los endpoints que expone son cinco:

| Endpoint | Método | Qué hace |
|---|---|---|
| `/` | GET | Devuelve `{status: ok, service: "adtech-recommendations-api"}`. |
| `/health` | GET | Health check sin tocar la base. |
| `/recommendations/{advertiser_id}/{modelo}` | GET | Última recomendación del advertiser para el modelo (TopCTR o TopProduct). |
| `/history/{advertiser_id}/` | GET | Recomendaciones disponibles en la base, filtradas por los últimos 7 días. **Ver nota.** |
| `/stats/` | GET | Estadísticas agregadas, incluyendo similaridad Jaccard entre modelos. |

**Nota sobre `/history/`:** la query SQL filtra por `date >= (hoy - 7 días)`, pero como las tablas se reescriben en cada run del DAG, en la práctica solo hay un día en la base. Por eso este endpoint devuelve un único día. Lo dejamos documentado como mejora pendiente en la sección 9.

Los códigos HTTP son los estándar de REST: 200 para respuestas exitosas, 400 cuando piden un modelo que no existe, 404 cuando no hay datos para ese advertiser, y 500 si algo se rompe en el server (en cuyo caso la traza queda en Cloud Logging y se puede debuggear).

---

## 5. Modelos de recomendación

### 5.1 TopCTR

El modelo TopCTR ordena los productos del advertiser por su click-through ratio (CTR), que es la fracción de impresiones que terminaron en click. Para un producto p y un advertiser a, lo calculamos así:

```
CTR(a, p) = clicks(a, p) / impresiones(a, p)
```

Para evitar la división por cero, el código usa `if r["impressions_count"] > 0 else 0`. Esto significa que productos sin impresiones obtienen CTR=0 automáticamente.

**Importante:** el código actual NO aplica un umbral mínimo de impresiones para descartar productos con muestras chicas. Si un producto tiene 2 impresiones y 2 clicks, su CTR es 1.0 y puede aparecer primero en el ranking, aunque la muestra sea estadísticamente débil. Lo notamos al revisar el código y lo dejamos documentado como primera mejora futura en la sección 9. Para el dataset usado, los volúmenes son lo suficientemente altos como para que este efecto sea menor en la práctica, pero es una limitación reconocida.

Para cada advertiser nos quedamos con los veinte productos con mejor CTR.

### 5.2 TopProduct

El modelo TopProduct va por el otro lado: ordena los productos por cantidad absoluta de visualizaciones (en `product_views`), sin importar si esas visualizaciones generaron clicks o no. Para cada advertiser nos quedamos con los veinte productos más vistos del día.

Este modelo captura una señal totalmente distinta. Un producto puede tener muchísimas vistas y bajo CTR (productos populares pero no muy bien orientados a la audiencia), o al revés. Por eso ambos modelos son complementarios y no redundantes.

### 5.3 Comparación entre modelos: similaridad de Jaccard

Tener dos modelos que recomiendan productos para el mismo advertiser nos dejó una pregunta natural: ¿qué tan parecidas son sus recomendaciones? Si fueran muy parecidas, mantener los dos sería redundante; si fueran muy distintas, los dos aportan información complementaria. Para responder esto usamos una métrica clásica de comparación entre conjuntos: el **coeficiente de similaridad de Jaccard**.

#### Qué es Jaccard

La idea es simple: medir cuánto se solapan dos conjuntos. Si tenemos un conjunto A y un conjunto B, Jaccard mira cuántos elementos están en los dos (intersección) y los divide por cuántos elementos están en cualquiera (unión):

```
Jaccard(A, B) = |A ∩ B| / |A ∪ B|
```

El resultado es un número entre 0 y 1, donde **0 significa "los conjuntos no comparten nada"** y **1 significa "son exactamente iguales"**.

Con un ejemplo concreto, supongamos dos canastas de frutas:

```
Canasta A: { manzana, pera, banana, kiwi, uva }
Canasta B: { banana, kiwi, naranja, frutilla, melon }

Intersección (frutas en las dos): { banana, kiwi } → 2 elementos
Unión (frutas en cualquiera):     8 elementos

Jaccard = 2 / 8 = 0.25
```

Las dos canastas comparten el 25% de las frutas si las consideramos juntas.

#### Cómo lo aplicamos al problema

A es el conjunto de los veinte productos recomendados por TopCTR para un advertiser, y B es el conjunto recomendado por TopProduct para el mismo advertiser. El cálculo se hace en el endpoint `/stats/`:

```python
ctr_set  = set(productos_topctr)
prod_set = set(productos_topproduct)
overlap  = len(ctr_set & prod_set)   # intersección
union    = len(ctr_set | prod_set)   # unión
jaccard  = overlap / union if union else 0
```

#### Cómo se interpretan los valores

```
0.00 - 0.10  Modelos casi disjuntos. Recomiendan productos muy distintos.
0.10 - 0.30  Solapamiento bajo. Comparten algunos productos pero
             la mayoría son distintos.
0.30 - 0.60  Solapamiento medio.
0.60 - 0.90  Solapamiento alto.
0.90 - 1.00  Casi idénticos. Uno sería redundante.
```

En la corrida de evaluación, los valores de Jaccard que arrojó la API se ubicaron entre **0.053 y 0.212** entre los veinte advertisers procesados. Es decir, los dos modelos están viendo señales bastante distintas: en promedio comparten entre el 5% y el 21% de los productos. La conclusión es que tener ambos modelos en el sistema aporta valor real, no son redundantes.

Desde el punto de vista del negocio: TopCTR encuentra productos que convierten bien cuando se muestran, mientras que TopProduct encuentra productos que tienen mucha exposición. Son objetivos complementarios y los anunciantes pueden usar uno u otro según optimicen por conversión o por alcance.

---

## 6. Despliegue e infraestructura

### 6.1 Containerización con Docker

La API se empaqueta en un container Docker para que corra igual en cualquier lado. La imagen se construye con un Dockerfile cortito basado en `python:3.12-slim`. Una decisión chiquita pero importante es el orden de las instrucciones: copiamos primero `requirements.txt` y hacemos el `pip install`, y recién después copiamos el código. Así, si solo cambia el código, Docker reutiliza el caché del pip install y no reinstala todo cada vez.

El container expone el puerto 8080 y arranca uvicorn con bind `0.0.0.0`, que es necesario para que Cloud Run pueda recibir requests externos. Si dejáramos `127.0.0.1`, el container solo se hablaría a sí mismo.

### 6.2 Pipeline Cloud Build → Artifact Registry → Cloud Run

El despliegue tiene tres pasos manuales pero versionados:

```
1. Build:    gcloud builds submit api/ --tag=...:vN
2. Push:     (lo hace solo Cloud Build al final del build)
3. Deploy:   gcloud run deploy fastapi-tp --image=...:vN
```

Cloud Build agarra la carpeta `api/`, la sube a un bucket temporal, levanta una VM efímera con Docker, ejecuta cada paso del Dockerfile y sube la imagen final a Artifact Registry. Tarda alrededor de un minuto. Una vez que la imagen está arriba, `gcloud run deploy` le dice a Cloud Run que use esa nueva imagen, crea una revisión nueva e inmutable y le manda el tráfico al container nuevo. No hay downtime: Cloud Run no apaga el container viejo hasta que el nuevo está respondiendo bien.

### 6.3 Versionado de imágenes y rollback

Cada deploy lo hicimos con un tag explícito (v1, v2, v3, v4) en lugar del tag `:latest`. Esto nos sirvió para dos cosas. Primero, llevar registro de qué se desplegó cuándo. Segundo, poder hacer rollback inmediato si algo se rompe: basta con redeployar la versión anterior porque la imagen sigue viva en Artifact Registry.

### 6.4 Variables de entorno y secretos

La conexión a Cloud SQL la configuramos por la variable de entorno `POSTGRES_URI`, definida directamente en el servicio Cloud Run. Esto sigue el principio de Twelve-Factor App: la configuración va por env vars, no commiteada al código. En el repositorio tenemos un archivo `.env.example` que muestra el formato esperado con un placeholder `TU_PASSWORD`.

La API tiene una mini capa que normaliza la URI antes de usarla. Si llega con un prefijo extendido del estilo `postgresql+psycopg2://`, la traduce al formato estándar (`postgresql://`) que entiende psycopg2 directamente. Lo agregamos cuando descubrimos que la URI ya seteada en el servicio venía con ese prefijo. Lo contamos con detalle en la sección 8.

### 6.5 Networking e IPs

La VM de Airflow tiene una IP externa estática (`34.55.205.251`), reservada como recurso aparte en GCP. Sin esto, cualquier reinicio podría cambiarle la IP y la URL quedaría rota.

La regla de firewall `allow-airflow-ui` permite tráfico entrante TCP en el puerto 8080 desde cualquier origen. Es aceptable porque Airflow tiene su propia autenticación.

La API en Cloud Run se expone a través de una URL HTTPS estable que provee la plataforma. No tuvimos que gestionar certificados SSL ni IPs externas: todo eso lo hace Cloud Run debajo.

---

## 7. Decisiones técnicas que tomamos en el camino

### 7.1 Endpoints con path params en lugar de query params

La primera versión de la API tenía endpoints con la forma `/recommendations/{advertiser_id}?model=TopCTR`. Cuando releímos la consigna nos dimos cuenta que pedía el modelo como **path parameter**: `/recommendations/{advertiser_id}/{modelo}`. Reescribimos los endpoints. Esto nos forzó también a agregar validación: si alguien manda un modelo que no existe, devolvemos 400 Bad Request.

### 7.2 psycopg2 directo en la API, SQLAlchemy via pandas en el DAG

La API usa psycopg2 directo. El DAG usa `pandas.to_sql`, que internamente requiere un engine de SQLAlchemy. Es una asimetría: dos componentes del mismo sistema usan dos abstracciones distintas para hablarle a la misma base.

La justificación es histórica: el DAG arrancó con la receta clásica de pandas-a-Postgres (`to_sql`), y la API arrancó separadamente con `psycopg2.connect`. Cuando lo notamos, decidimos no unificar para no introducir cambios de último momento.

### 7.3 Cloud Run en lugar de una VM dedicada para la API

Para la API también podríamos haber usado una VM, pero elegimos Cloud Run porque tiene autoscaling automático, escala a cero cuando no hay tráfico, y maneja TLS automáticamente. La contra es la latencia de cold start: el primer request después de un rato de inactividad puede tardar uno o dos segundos extra. Para los volúmenes que esperamos en este TP, no es problema.

### 7.4 Versionar imágenes Docker con vN en vez de :latest

Las imágenes Docker las subimos a Artifact Registry con tags explícitos (v1, v2, v3, v4). Esto agrega trabajo manual mínimo, pero a cambio nos da rollback inmediato si una versión nueva trae bugs.

---

## 8. Lo que se nos rompió y cómo lo resolvimos

### 8.1 La connection string que la API no entendía

**Síntoma:** después de un deploy, los endpoints empezaron a tirar 500 Internal Server Error. `/health` respondía OK, pero cualquier endpoint que tocara la base explotaba.

**Diagnóstico:** los logs en Cloud Run mostraban:

```
psycopg2.ProgrammingError: invalid dsn: missing "=" after
"postgresql+psycopg2://..." in connection info string
```

La URI que estaba seteada en la variable de entorno tenía un prefijo extendido (`postgresql+psycopg2://`) que SQLAlchemy entiende pero psycopg2 directo no. Cuando psycopg2 ve un string que no arranca con `postgresql://` limpio, intenta interpretarlo como un DSN tipo `clave=valor` y falla.

**Solución:** agregar al inicio de `main.py`:

```python
if POSTGRES_URI.startswith("postgresql+psycopg2://"):
    POSTGRES_URI = POSTGRES_URI.replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )
```

Así el código se vuelve robusto y no tuvimos que tocar la variable de entorno en Cloud Run.

### 8.2 El scheduler de Airflow huérfano sin variables de entorno

**Síntoma:** durante varios días el DAG no procesó datos nuevos. La fecha más reciente que devolvía la API se quedó pegada en un día atrás.

**Diagnóstico:** corriendo `airflow dags list` vimos:

```
Error: Failed to load all files. For details, run
`airflow dags list-import-errors`
No data found
```

Y `list-import-errors` mostró:

```
RuntimeError: Falta la variable de entorno POSTGRES_URI.
```

La variable estaba en `~/.bashrc`, sí. El tema es que `.bashrc` solo lo lee una shell interactiva. El proceso del scheduler había arrancado en algún momento desde una shell que tenía la variable, pero después esa shell se cerró y el scheduler quedó **huérfano** (PPID=1, adoptado por init). Cuando Airflow internamente recicla workers, esos no heredan más la variable de la shell original.

**Solución:** matar todos los procesos viejos, abrir una shell limpia que cargara `.bashrc`, y volver a arrancar el scheduler y el webserver. Para una solución más definitiva habría que crear unidades systemd con la variable definida en el archivo `.service`. Lo dejamos como mejora futura.

### 8.3 Confusión con el nombre del DAG

**Síntoma:** intentamos ver runs históricos del DAG con `airflow dags list-runs -d adtech_recos` y devolvía vacío.

**Diagnóstico:** en el código, el `dag_id` estaba puesto como `adtech_pipeline`, no `adtech_recos`. Habíamos cambiado el nombre en algún punto del desarrollo y nos quedó la referencia vieja en notas.

**Lección:** los nombres son contratos. Si el `dag_id` en el código no matchea con lo que esperan los comandos o la documentación, todo se rompe. Siempre revisar el código fuente como fuente de verdad.

---

## 9. Limitaciones y mejoras pendientes

El sistema cumple los objetivos pero tiene áreas con margen de mejora. Las dejamos documentadas porque vale la pena ser honestos sobre el estado real:

- **Tablas reescritas en cada run del DAG:** el `db_writing` usa `pandas.to_sql(if_exists="replace")`, por lo que las tablas se borran y recrean enteras en cada corrida. Como consecuencia, `/history/{advertiser_id}/` devuelve un solo día en lugar de los siete que sugiere la firma del endpoint. La mejora directa sería cambiar a un esquema con `DELETE WHERE date = ds` + `INSERT`, manteniendo histórico real.

- **TopCTR sin filtro de impresiones mínimas:** el modelo no descarta productos con pocas impresiones. Un producto con 2 impresiones y 2 clicks tiene CTR=1.0. La mejora directa sería agregar un filtro `impressions >= N` antes de calcular el ranking.

- **DAG con SQLAlchemy via pandas, API con psycopg2 directo:** dos formas de hablarle a la misma base. Una mejora prolija sería unificar ambos componentes.

- **El DAG no descarga directamente de GCS:** lee CSVs locales de `/home/pipeposse/trabajo_practico/`. Hay que bajar los archivos manualmente con `gsutil cp`. Una mejora sería usar `google-cloud-storage` desde el DAG para que cada run descargue lo necesario.

- **Tests automatizados:** la API y el DAG no tienen tests unitarios ni de integración. En producción real escribiríamos tests con pytest. Los validamos manualmente con curl.

- **Lock file de dependencias:** el `requirements.txt` tiene versiones pineadas (`==`), pero no incluye hashes ni dependencias transitivas explícitas. Una mejora sería usar pip-compile o Poetry.

- **Conexión a Cloud SQL más segura:** la API se conecta a la base por IP pública. Cloud Run permite conectarse via Cloud SQL Auth Proxy.

- **Monitoreo y alertas:** no tenemos alertas configuradas para fallos del DAG ni del servicio.

- **Autenticación de la API:** los endpoints son públicos. Para un escenario real habría que poner API keys, OAuth o IAM.

- **Servicio systemd para Airflow:** el scheduler y webserver corren con `nohup`. Lo más prolijo serían archivos `.service` de systemd para que se reinicien al boot de la VM.

---

## 10. Conclusiones

Este TP nos llevó por todo el ciclo de un sistema de datos en producción: desde archivos crudos hasta una API HTTPS pública pasando por orquestación, base de datos relacional, containers, deploy versionado, y debugging de errores reales. Cada pieza era un mundo aparte, y juntarlas en un sistema que corre de punta a punta fue donde más aprendimos.

Los servicios administrados de GCP que usamos (Cloud Storage, Compute Engine, Cloud SQL, Artifact Registry, Cloud Build, Cloud Run) cubrieron los componentes de infraestructura sin que tuviéramos que pelearnos con servidores propios. Pudimos enfocar el tiempo en la lógica del pipeline y en el código.

Los dos modelos que implementamos, TopCTR y TopProduct, nos sorprendieron al medir su similaridad con Jaccard. Esperábamos quizás que fueran más parecidos, pero los valores entre 0.053 y 0.212 nos mostraron que están capturando señales distintas. Tener ambos modelos aporta valor real.

Más allá de lo funcional, lo más valioso del trabajo fue lo que aprendimos del oficio: cómo se lee un stack trace en logs centralizados, por qué un proceso huérfano pierde sus variables de entorno, cómo Cloud Build genera capas Docker reutilizables, qué significa realmente que un servicio sea stateless. Son cosas que en clase se mencionan rápido y acá las vivimos en carne propia.

Nos vamos del TP con una caja de herramientas más grande y, sobre todo, con un proyecto que podemos abrir y entender de punta a punta, incluyendo sus limitaciones. Eso era el objetivo desde el principio.

---

## 11. Anexos

### A. URLs públicas y credenciales para evaluación

| Recurso | Acceso |
|---|---|
| API REST (Cloud Run) | https://fastapi-tp-xbo6kajhza-uc.a.run.app |
| Documentación interactiva | https://fastapi-tp-xbo6kajhza-uc.a.run.app/docs |
| Airflow UI | http://34.55.205.251:8080 |
| Repositorio Git | https://github.com/pipeposse/tp-final-adtech |

Credenciales de Airflow para evaluación (rol Viewer, solo lectura):

```
Usuario:  profesor
Password: profesor
```

### B. Comandos de despliegue

Construcción de imagen Docker via Cloud Build:

```bash
gcloud builds submit api/ \
  --tag=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:vN \
  --project=tp-final-adtech-493922
```

Despliegue a Cloud Run:

```bash
gcloud run deploy fastapi-tp \
  --image=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:vN \
  --region=us-central1 \
  --project=tp-final-adtech-493922
```

Reserva de IP estática para la VM de Airflow:

```bash
gcloud compute addresses create diegue-pipe-belu-airflow-ip \
  --addresses=34.55.205.251 \
  --region=us-central1 \
  --project=tp-final-adtech-493922
```

Creación del usuario profesor en Airflow:

```bash
/home/pipeposse/airflow_venv/bin/airflow users create \
  --username profesor --firstname Profesor --lastname TPFinal \
  --role Viewer --email profesor@tp-adtech.local --password 'profesor'
```

### C. Ejemplos de respuestas de la API

Respuesta del endpoint `/recommendations/{advertiser_id}/TopCTR`:

```json
{
  "advertiser_id": "LW045DVYSGRD75TK6U54",
  "model": "TopCTR",
  "date": "2026-05-01",
  "recommendations": ["0639y2", "1mlwn4", "2bx2zk", "..."]
}
```

Fragmento de respuesta de `/stats/`, sección de coincidencia entre modelos:

```json
"coincidencia_entre_modelos": [
  { "advertiser_id": "LW045DVYSGRD75TK6U54",
    "productos_en_comun": 7, "jaccard": 0.212 },
  { "advertiser_id": "OY5LNPB5A8FF43ITRZG3",
    "productos_en_comun": 7, "jaccard": 0.212 }
]
```
