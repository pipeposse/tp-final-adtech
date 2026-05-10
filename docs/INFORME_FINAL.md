# Pipeline de Recomendaciones AdTech

**Trabajo Práctico Final · Programación Avanzada para Grandes Volúmenes de Datos**
Universidad de San Andrés

**Autores:** Felipe Posse · Diego Sanguinetti · Belén Candela Lozada Montanari
**Profesores:** Agustín Mosteiro · Matías Dinota
**Fecha de entrega:** 9 de mayo de 2026

---

## Índice

1. [Introducción](#1-introducción)
2. [Objetivos y alcance](#2-objetivos-y-alcance)
3. [Arquitectura del sistema](#3-arquitectura-del-sistema)
4. [Las piezas, una por una](#4-las-piezas-una-por-una)
5. [Los dos modelos de recomendación](#5-los-dos-modelos-de-recomendación)
6. [Despliegue e infraestructura](#6-despliegue-e-infraestructura)
7. [Decisiones tomadas durante el desarrollo](#7-decisiones-tomadas-durante-el-desarrollo)
8. [Problemas encontrados y resolución](#8-problemas-encontrados-y-resolución)
9. [Limitaciones actuales y trabajo futuro](#9-limitaciones-actuales-y-trabajo-futuro)
10. [Conclusiones](#10-conclusiones)
11. [Anexos](#11-anexos)

---

## 1. Introducción

El objetivo del siguiente trabajo práctico fue crear un sistema que analice datos de impresiones y clicks de publicidad online y pueda informarle a cada anunciante (a partir de ahora los llamaremos *advertisers*) cuáles son sus 20 productos más interesantes según dos formas distintas de medirlo. El sistema corre automáticamente, todos los días, sin necesidad de ejecutarlo manualmente cada vez.

Por debajo, el sistema se compone de cinco piezas principales: un bucket en la nube donde se reciben los archivos crudos (un bucket es básicamente una carpeta gigante online), una máquina virtual que corre Apache Airflow (la "agenda" que decide cuándo correr cada tarea), una base de datos PostgreSQL administrada por Google donde se guardan los resultados, una API web que devuelve esos resultados cuando alguien la consulta, y todo desplegado en Google Cloud Platform.

Se implementaron los dos modelos solicitados: **TopCTR**, que ordena los productos según su *click-through rate* (CTR), es decir, la proporción de clicks obtenidos respecto de la cantidad de impresiones; y **TopProduct**, que los clasifica en función de la cantidad total de visualizaciones.

Adicionalmente, se calculó la similitud entre las recomendaciones generadas por ambos modelos utilizando la similaridad de Jaccard, una métrica ampliamente utilizada para comparar conjuntos. Más adelante se detalla su definición e interpretación.

Toda la infraestructura está en Google Cloud y se administra desde la consola web o con el comando `gcloud`. La API la empaquetamos en una imagen Docker (un "tupper" que tiene adentro la app y todo lo que necesita), la subimos a un registry de imágenes, y la deployamos en Cloud Run, que es un servicio que corre containers Docker detrás de una URL HTTPS sin que tengamos que mantener un servidor.

---

## 2. Objetivos y alcance

### 2.1 Contexto del problema

La industria AdTech (publicidad digital programática) genera diariamente cantidades enormes de eventos. Cada vez que un usuario ve un anuncio se registra una impresión, y si interactúa con él, se registra un click. Los anunciantes necesitan información procesada sobre qué productos están teniendo mejor desempeño para decidir dónde invertir presupuesto, qué creatividades impulsar y a qué audiencias dirigir sus campañas.

La construcción de este tipo de pipeline integra varios conceptos que habitualmente se estudian por separado: la ingesta de datos, su procesamiento batch en ejecuciones programadas, el almacenamiento en bases de datos y la exposición de resultados mediante una API. Uno de los aspectos más interesantes del trabajo práctico es justamente la integración de todos estos componentes en un único sistema. Al mismo tiempo, esto introduce desafíos adicionales, ya que cada componente puede fallar de manera distinta y, en un sistema distribuido con múltiples piezas, resulta fundamental poder identificar rápidamente el origen de los problemas.

### 2.2 Objetivos del trabajo

- Diseñar y desplegar un pipeline completo en Google Cloud, desde el archivo crudo hasta una API pública.
- Orquestar el procesamiento diario con Apache Airflow, de manera que cada paso quede registrado y, si falla, se pueda reintentar sin tener que hacer todo de cero.
- Implementar los dos modelos pedidos (TopCTR y TopProduct) y, ya que estábamos, agregar un análisis de cuánto se parecen entre ellos.
- Construir una API REST en FastAPI que cumpla los endpoints de la consigna, valide los inputs (rechace cosas raras antes de llegar a la base) y use códigos HTTP correctos (200 para todo bien, 400 si te equivocás vos, 404 si no existe, 500 si por algún ajuste rompimos algo).
- Empaquetar la API en Docker y desplegarla en Cloud Run con tags versionados.

### 2.3 Alcance y estado final de implementación

El sistema procesa tres datasets de entrada por día: `active_advertisers` (los anunciantes que están "activos"), `ads_views` (impresiones y clicks del día), y `product_views` (visualizaciones de productos en el sitio). La salida es la tabla `recommendations` en la base, con las veinte mejores recomendaciones por advertiser y por modelo. La API expone consultas individuales, históricas y de estadísticas.

El proyecto pasó por dos iteraciones mayores durante el desarrollo. Una primera versión utilizaba un esquema "denormalizado" con dos tablas separadas (`top_ctr` y `top_product`) y los productos guardados como lista serializada en una columna TEXT. La versión final, deployada el 2026-05-09, migró a un esquema normalizado con una sola tabla `recommendations` (una fila por producto) y upsert con `ON CONFLICT`, lo que habilita histórico real de recomendaciones y queries SQL convencionales. Ambas mejoras se documentan en las secciones 4, 7 y 8.

---

## 3. Arquitectura del sistema

### 3.1 Visión general de la arquitectura

El sistema fue dividido en cuatro capas con responsabilidades claramente separadas. Esta decisión permite aislar problemas con mayor facilidad y simplifica las tareas de mantenimiento y debugging. La arquitectura final quedó organizada de la siguiente manera:

```
GCS bucket (los CSVs crudos viven aca, en gs://tp-final-adtech/)
   |
   v
Compute Engine VM (e2-small, una maquina virtual chica en us-central1-a)
   |-- Apache Airflow (LocalExecutor, que corre todo en la misma VM)
       |-- scheduler (el que decide cuando dispara el DAG)
       |-- webserver (la UI web en el puerto 8080)
       |-- DAG adtech_pipeline_v2_gcs (corre todos los dias 02:00 UTC, es la segunda versión que deployamos)
           |-- FiltrarDatos
           |-- TopCTR
           |-- TopProduct
           |-- DBWriting
   |
   v
Cloud SQL (PostgreSQL 18 administrado por Google)
   |-- airflow_db (donde Airflow guarda su propia metadata)
   |-- recos_db
       |-- recommendations (una fila por (advertiser, modelo, producto, dia, tambíen nuevo modelo)
       |-- api_logs (registro de cada consulta a la API)
   ^
   |
Cloud Run (la API FastAPI, dentro de una imagen Docker fastapi-tp:vN)
   |
   v
Cliente HTTP ( nosotros, o cualquiera con la URL)
```

### 3.2 El recorrido del dato, de punta a punta

Los CSVs crudos viven en el bucket `gs://tp-final-adtech/`. El DAG los lee directamente desde ese bucket (sin necesidad de descargas intermedias a la VM) usando el cliente `google-cloud-storage` desde el código del DAG.

Cada día a las **02:00 UTC**, el scheduler de Airflow dispara el DAG `adtech_pipeline_v2_gcs` con la fecha lógica correspondiente (que en Airflow se llama `ds`, *date string*, formato `YYYY-MM-DD`). Las cuatro tareas se ejecutan en este orden: primero **FiltrarDatos** lee los CSVs del día desde GCS y se queda solo con los advertisers que están en `active_advertisers.csv`, dejando los archivos filtrados en `/tmp` de la VM. Después **TopCTR** y **TopProduct** corren en paralelo, cada uno calcula su ranking de top 20 productos por advertiser. Al final, **DBWriting** agarra los dos rankings y los escribe a Postgres mediante una operación de upsert (`INSERT ... ON CONFLICT (advertiser_id, model, product_id, date) DO UPDATE`), de modo que las corridas de cada día se acumulan sin pisar las anteriores.

Por otro lado, totalmente desacoplado, el servicio FastAPI en Cloud Run consulta la tabla `recommendations` cuando recibe un request HTTP. Cada vez que devuelve una recomendación, registra la consulta en una tabla auxiliar llamada `api_logs`. Esa tabla se usa en el endpoint `/stats/` para mostrar uso de la API. La separación entre el batch (que procesa una vez al día) y el servicio HTTP (que está disponible 24x7) es muy típica en sistemas reales: nos permite escalar y deployar cada uno por su lado.

---

## 4. Las piezas, una por una

### 4.1 Cloud Storage: dónde caen los archivos

Los archivos CSV crudos se almacenan en un bucket de Google Cloud Storage (GCS), el servicio de almacenamiento de objetos de Google Cloud. Este tipo de almacenamiento resulta adecuado para datos semiestructurados como archivos CSV.
Los archivos siguen un patrón de nombres por fecha:

- `active_advertisers.csv` (la lista de anunciantes vivos, no cambia por dia).
- `ads_views_YYYY-MM-DD.csv` (impresiones y clicks del dia).
- `product_views_YYYY-MM-DD.csv` (visualizaciones de productos del dia).


### 4.2 Apache Airflow: el orquestador

Airflow corre sobre una máquina virtual `e2-small` en `us-central1-a` de Compute Engine. Un orquestador es un programa que decide qué tarea correr, en qué orden, y qué hacer si una falla. Sin un orquestador, el sistema dependería de scripts ejecutados mediante tareas programadas tradicionales, lo que dificulta el monitoreo, el manejo de errores y la recuperación ante fallos. El uso de Apache Airflow permite centralizar la ejecución, trazabilidad y reintento de las tareas del pipeline.

La principal desventaja de esta decisión es que el mantenimiento operativo queda completamente a cargo del equipo. Por ejemplo, si la máquina virtual se reinicia, es necesario volver a levantar manualmente los servicios correspondientes, y cualquier problema relacionado con la metadata interna de Airflow debe resolverse de manera directa.
La instalación de Airflow se realizó dentro de un entorno virtual de Python (venv) ubicado en `/home/pipeposse/airflow_venv`, lo que permite aislar las dependencias del proyecto respecto del resto del sistema. La metadata interna de Airflow —incluyendo información sobre DAGs, ejecuciones y logs de tareas— se almacena en una base de datos PostgreSQL denominada `airflow_db`, alojada en la instancia de Cloud SQL del proyecto.

El DAG actual se llama `adtech_pipeline_v2_gcs`. Vale aclararlo porque durante el desarrollo lo llamamos sucesivamente `adtech_recos`, `adtech_pipeline` y finalmente `adtech_pipeline_v2_gcs` (el sufijo `_v2_gcs` marca la migración a lectura directa desde Cloud Storage). Si aparecen referencias viejas a `adtech_recos` o `adtech_pipeline` en notas internas, son artefactos del desarrollo. El nombre vivo es `adtech_pipeline_v2_gcs`. Al principio los archivos vivían en la virtual machine, cuando vimos que todo funcionaba bien hicimos la mudanza a GSC.

Las cuatro tareas con sus dependencias quedaron de la siguiente manera: `FiltrarDatos` primero, después `TopCTR` y `TopProduct` en paralelo, y al final `DBWriting` que junta los resultados. La fecha que procesa cada run viene en el contexto de Airflow como `ds`, y el código usa esa fecha para construir el nombre de los archivos a leer (por ejemplo `ads_views_2026-04-19.csv`).

La interfaz web de Apache Airflow se encuentra expuesta en el puerto 8080 de la máquina virtual. Con el objetivo de permitir la inspección del sistema sin comprometer su operación, se creó un usuario adicional con permisos de tipo *Viewer* (solo lectura). Este usuario puede visualizar los DAGs y sus ejecuciones, pero no posee permisos para disparar tareas manualmente, pausar pipelines ni modificar configuraciones.

### 4.3 Cloud SQL: la base de datos

Cloud SQL es PostgreSQL administrado por Google. En lugar de instalar Postgres en una máquina y mantenerlo a mano, Google te lo entrega: con backups, parches de seguridad y monitoreo incluidos.

Una misma instancia de Cloud SQL puede alojar múltiples bases de datos lógicas. En este proyecto se utilizaron dos bases separadas: `airflow_db`, destinada exclusivamente a la metadata interna de Apache Airflow, y `recos_db`, que contiene los datos propios del sistema de recomendaciones.

Esta separación permite aislar las responsabilidades de cada componente y reduce riesgos operativos. Por ejemplo, una migración interna de Airflow podría modificar tablas de su propia metadata, por lo que mantener ambas bases desacopladas evita posibles interferencias con los datos del proyecto. Además, facilita la administración de backups y tareas de mantenimiento específicas para cada entorno.

Dentro de `recos_db` el schema final tiene **dos tablas**:

```sql
CREATE TABLE recommendations (
    id            BIGSERIAL PRIMARY KEY,
    advertiser_id VARCHAR(64)  NOT NULL,
    model         VARCHAR(32)  NOT NULL,    -- 'top_ctr' o 'top_product'
    product_id    VARCHAR(128) NOT NULL,
    rank          INTEGER      NOT NULL,
    score         NUMERIC(10,6),
    date          DATE         NOT NULL,
    created_at    TIMESTAMP    DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_recommendations_natural_key
    ON recommendations (advertiser_id, model, product_id, date);

CREATE TABLE api_logs (
    id            SERIAL PRIMARY KEY,
    advertiser_id TEXT,
    model         TEXT,
    date          TEXT,
    timestamp     TIMESTAMP DEFAULT NOW()
);
```
##La mejora
La tabla `recommendations` es **normalizada**: una fila por (advertiser, modelo, producto, día). Esto permite hacer queries SQL convencionales (filtrar, ordenar, contar, indexar) sobre cada producto individual, en lugar de tener que parsear strings serializados en el cliente. Eséticamente termino siendo un cambio muy beneficioso.

El índice único `uq_recommendations_natural_key` es necesario para que la operación de upsert del DAG (`INSERT ... ON CONFLICT (advertiser_id, model, product_id, date)`) funcione: PostgreSQL exige un constraint único o un índice único sobre exactamente esas columnas para resolver el conflicto. Gracias a este diseño, el DAG puede correr el mismo día más de una vez (por ejemplo, en caso de reintento) sin duplicar filas: encuentra la combinación existente por la clave natural y actualiza el `rank`, `score` y `created_at`. (Nos costó mucho entender porque no funcionaba sin el index)

La tabla `api_logs` la crea la API automáticamente al arrancar (en una función de inicialización llamada `lifespan`, que corre una sola vez cuando el container arranca). Registra cada consulta exitosa de recomendaciones, lo cual alimenta el endpoint `/stats/`. Además crel la tabla si no esta creada.

> **Nota histórica.** Una versión anterior del schema utilizaba dos tablas (`top_ctr` y `top_product`) con una columna TEXT que guardaba la lista de los 20 productos serializada como literal Python (`"['p1', 'p2', ...]"`). Esa decisión simplificaba las queries pero introducía un anti-patrón relacional: era imposible filtrar, ordenar o contar productos individuales sin parsear strings en el cliente. La migración al esquema normalizado se realizó el 2026-05-09 junto con el redeploy de la API a su versión 3 (ver secciones 7 y 8). Las tablas viejas fueron dropeadas tras confirmar que ningún cliente seguía consultándolas.

### 4.4 La API FastAPI corriendo en Cloud Run

La API fue desarrollada utilizando FastAPI 0.115, un framework de Python orientado a la construcción de APIs web. FastAPI permite definir endpoints HTTP a partir de funciones estándar de Python mediante el uso de decoradores como `@app.get(...)`. Además, genera automáticamente documentación interactiva de la API, accesible a través del endpoint `/docs`.

Para servir las peticiones por HTTP, FastAPI necesita un servidor (no es un servidor por sí mismo, es solo el framework). Usamos Uvicorn en su versión "standard", que viene con dependencias adicionales que mejoran la performance. FastAPI sin Uvicorn es una API escrita, pero no encendida para recibir consultas.

La API se conecta a PostgreSQL utilizando `psycopg2-binary` de manera directa, sin capas adicionales de abstracción. `psycopg2` es el driver de PostgreSQL para Python y permite establecer la comunicación entre la aplicación y la base de datos.

En este proyecto se decidió no utilizar un ORM (*Object Relational Mapper*), es decir, una capa que abstrae las consultas SQL mediante objetos de Python. Dado que las operaciones requeridas por la API son relativamente simples —principalmente consultas SELECT y registros INSERT—, se priorizó una implementación más liviana y con consultas SQL explícitas, facilitando la lectura y el control del código.

Un ejempollo de ORM:

class Recommendation(Base):
    __tablename__ = "recommendations"

    id = Column(Integer, primary_key=True)
    advertiser_id = Column(Integer)
    model_name = Column(String)
    product_id = Column(Integer)
    date = Column(Date)
    score = Column(Float)

    recommendations = session.query(Recommendation).filter(
    Recommendation.advertiser_id == advertiser_id
).all()

Como se la estructura es mucho mas compleja y pierde interpretabilidad.

#### Filosofía de las queries SQL

El nuevo `main.py` (versión `:v3`, deploy 2026-05-09) sigue una filosofía simple: **cada query debe poder explicarse en una oración**. No hay CTEs ni JOINs explícitos; la única "sofisticación" es una subquery escalar en `/stats` que devuelve la fecha más reciente. La lógica de matemática de sets (intersección, unión para Jaccard) vive en Python, donde es más legible. Realmente buscamos que las queries se puedan entender a simple vista.

Las queries fundamentales de cada endpoint son:

```sql
-- /recommendations/{advertiser_id}/{modelo}
-- Lectura: "los 20 productos del último día para ese advertiser+modelo"
SELECT product_id, date FROM recommendations
WHERE advertiser_id = %s AND model = %s
ORDER BY date DESC, rank
LIMIT 20;

-- /history/{advertiser_id}/
-- Lectura: "los productos del advertiser+modelo de los últimos 7 días"
SELECT date, product_id FROM recommendations
WHERE advertiser_id = %s AND model = %s AND date >= %s
ORDER BY date DESC, rank;

-- /stats/ → preparación de Jaccard
-- Lectura: "todas las filas del día más reciente, para todos los advertisers"
SELECT advertiser_id, model, product_id FROM recommendations
WHERE date = (SELECT MAX(date) FROM recommendations);
```

Para `/stats/`, después se agrupa en Python por `(advertiser_id, model)` armando dos sets de productos, y se calcula la intersección y unión:

```python
from collections import defaultdict
productos = defaultdict(lambda: {"top_ctr": set(), "top_product": set()})
for r in rows:
    productos[r["advertiser_id"]][r["model"]].add(r["product_id"])
# Por cada advertiser, |ctr ∩ prod| / |ctr ∪ prod|
```

El pipeline batch (DAG) también utiliza `psycopg2` directo, mediante el helper `psycopg2.extras.execute_values` que permite enviar un `INSERT` masivo con `ON CONFLICT` en una sola query. La asimetría menor que persiste es de configuración: el DAG lee las credenciales de variables de entorno separadas (`RECOS_DB_HOST`, `RECOS_DB_USER`, `RECOS_DB_PASSWORD`, `RECOS_DB_NAME`) mientras que la API consume una única `POSTGRES_URI`. Ambos terminan conectándose con el mismo driver al mismo Postgres.

#### Empaquetado y despliegue

El servicio se empaqueta como imagen Docker basada en `python:3.12-slim` (la versión "chica" de la imagen oficial de Python, ~50 MB en lugar de ~900 MB). Cloud Build (un servicio de Google que hace builds de Docker en la nube) se encarga de armar la imagen y subirla a Artifact Registry (como Docker Hub privado de Google) con un tag versionado: `v1`, `v2`, `v3`. Cloud Run consume esa imagen y la sirve detrás de una URL HTTPS estable.

#### Endpoints expuestos

| Endpoint | Método | Qué hace |
|---|---|---|
| `/` | GET | Devuelve `{status: ok, service: ...}`. Útil como ping. |
| `/health` | GET | Health check sin tocar la base. Devuelve `{status: ok}`. |
| `/recommendations/{advertiser_id}/{modelo}` | GET | Última recomendación del advertiser para el modelo (`TopCTR` o `TopProduct`). |
| `/history/{advertiser_id}/` | GET | Recomendaciones disponibles, filtradas por últimos 7 días. |
| `/stats/` | GET | Estadísticas agregadas, incluyendo similaridad Jaccard entre modelos. |

A partir de la migración al esquema normalizado, el endpoint `/history/` devuelve genuinamente hasta 7 días de historial cuando hay corridas del DAG dentro de esa ventana, ya que las corridas de cada día se acumulan en lugar de pisar las anteriores. Si la última corrida del DAG es más vieja que el cutoff, el endpoint devuelve 404 (no hay data en el rango), lo cual es correcto.

Los códigos HTTP utilizados por la API siguen las convenciones estándar de arquitecturas REST. Se devuelve código 200 cuando la consulta se procesa correctamente, 400 ante errores en los parámetros enviados por el cliente (por ejemplo, solicitar un modelo inexistente), 404 cuando no existen datos para el advertiser consultado, y 500 ante errores internos del servidor. En estos últimos casos, las trazas correspondientes quedan registradas en Cloud Logging, permitiendo realizar tareas de debugging y monitoreo.

---

## 5. Los dos modelos de recomendación

### 5.1 TopCTR: ordenar por click-through ratio

El modelo TopCTR ordena los productos del advertiser por su CTR (*click-through rate*), que es la fracción de impresiones que terminaron en click. La fórmula es directa:

```
CTR(advertiser, producto) = clicks / impresiones
```

Para evitar errores por división por cero durante el cálculo del CTR, se utilizó: `if r["impressions_count"] > 0 else 0`. Por esta razón, productos sin impresiones obtienen CTR = 0 automáticamente.

Es importante destacar una limitación conocida del modelo actual: no se aplica un umbral mínimo de impresiones para filtrar productos con bajo volumen de observaciones. Como consecuencia, un producto con únicamente dos impresiones y dos clicks obtiene un CTR = 1.0, es decir, el valor máximo posible, pudiendo posicionarse artificialmente en los primeros lugares del ranking a pesar de contar con evidencia estadísticamente débil.

Si bien en el dataset utilizado los volúmenes de datos son lo suficientemente altos como para que este efecto tenga un impacto reducido en la práctica, la ausencia de un filtro mínimo de impresiones constituye una limitación metodológica del sistema. Esta mejora fue identificada y documentada en la sección 9 como una de las principales líneas de trabajo futuro.

Para cada advertiser nos quedamos con los veinte productos con mejor CTR.

Este modelo prioriza productos que generan engagement: aunque tengan menos visualizaciones absolutas, son los que mejor convierten cuando se muestran. Es el modelo a elegir si el anunciante optimiza por conversión, no por alcance.

### 5.2 TopProduct: ordenar por cantidad de visualizaciones

El modelo TopProduct aborda el problema desde una perspectiva diferente, ordenando los productos según la cantidad absoluta de visualizaciones registradas en `product_views`, independientemente de si dichas visualizaciones derivaron en clicks. Para cada advertiser, el sistema selecciona los veinte productos con mayor cantidad de visualizaciones correspondientes al día procesado.

Este modelo captura una señal totalmente distinta a TopCTR. Un producto puede tener muchísimas vistas y bajo CTR (productos populares pero no muy bien orientados a la audiencia), o al revés. Por eso ambos modelos son complementarios y no redundantes: cada uno responde una pregunta diferente del negocio.

### 5.3 Comparando los dos modelos: similaridad de Jaccard

La implementación de dos modelos distintos de recomendación para un mismo advertiser plantea naturalmente la siguiente pregunta: ¿qué tan similares son las recomendaciones generadas por cada uno? Si ambos modelos produjeran resultados muy parecidos, mantener los dos podría resultar redundante. Por el contrario, si las recomendaciones fueran significativamente diferentes, cada modelo estaría aportando información complementaria.

Para analizar esta cuestión se utilizó una métrica clásica de comparación entre conjuntos: el coeficiente de similaridad de Jaccard. La idea es simple: medir cuánto se solapan dos conjuntos. Si tenemos un conjunto A y un conjunto B, Jaccard mira cuántos elementos están en los dos (intersección) y los divide por cuántos elementos están en cualquiera (unión):

```
Jaccard(A, B) = |A ∩ B| / |A ∪ B|
```

El resultado es un número entre 0 y 1, donde 0 significa "los conjuntos no comparten nada" y 1 significa "son exactamente iguales".

En este trabajo, el conjunto A son los 20 productos recomendados por TopCTR para un advertiser, y el conjunto B, los 20 recomendados por TopProduct para el mismo advertiser. El cálculo lo hacemos en el endpoint `/stats/` con código Python bien compacto:

```python
ctr_set  = set(productos_topctr)
prod_set = set(productos_topproduct)
overlap  = len(ctr_set & prod_set)   # interseccion (operador & en sets)
union    = len(ctr_set | prod_set)   # union (operador | en sets)
jaccard  = overlap / union if union else 0
```

#### Cómo interpretar los valores

| Rango Jaccard | Interpretación |
|---|---|
| 0.00 - 0.10 | Modelos casi disjuntos (recomiendan productos muy distintos) |
| 0.10 - 0.30 | Solapamiento bajo |
| 0.30 - 0.60 | Solapamiento medio |
| 0.60 - 0.90 | Solapamiento alto |
| 0.90 - 1.00 | Casi idénticos. Uno sería redundante |

En la corrida de evaluación que hicimos al cierre del proyecto, los valores de Jaccard que arrojó la API se ubicaron entre **0.081 y 0.333** entre los veinte advertisers procesados. Esto significa que los dos modelos comparten en promedio entre el 8% y el 33% de los productos recomendados. La conclusión práctica es que tener ambos modelos en el sistema aporta valor real, no son redundantes. Si fueran idénticos (Jaccard cerca de 1) podríamos descartar uno.

Desde una perspectiva de negocio, ambos modelos responden a objetivos distintos pero complementarios. TopCTR prioriza productos con alta capacidad de conversión una vez mostrados, mientras que TopProduct identifica productos con mayor nivel de exposición y visibilidad. Dependiendo de la estrategia del anunciante, cada enfoque puede resultar más adecuado según se busque optimizar conversión, alcance o reconocimiento de producto.

---

## 6. Despliegue e infraestructura

### 6.1 Empaquetar la API con Docker

La API se empaqueta en un container Docker para que corra igual en cualquier lado: en nuestra máquina, en la del corrector, en Cloud Run. La imagen se construye con un `Dockerfile` cortito (8 líneas) basado en `python:3.12-slim`.

Una decisión importante fue copiar primero el archivo `requirements.txt` y ejecutar `pip install` antes de copiar el código de la aplicación. Esto permite que Docker reutilice el caché de capas cuando solo cambia el código fuente, evitando reinstalar todas las dependencias en cada build y reduciendo considerablemente los tiempos de construcción.

El container expone el puerto 8080 y arranca uvicorn con el bind `0.0.0.0`. Esto último es importante: `0.0.0.0` significa "escuchá en todas las interfaces de red", lo cual es necesario para que Cloud Run pueda recibir requests externos. Si dejáramos `127.0.0.1` (localhost), el container solo se hablaría a sí mismo y nadie de afuera podría llegarle.

### 6.2 El pipeline de deploy: build, push, deploy

El despliegue tiene tres pasos manuales pero versionados:

1. **Build:** `gcloud builds submit api/ --tag=...:vN`
2. **Push:** lo hace Cloud Build solo al final del build.
3. **Deploy:** `gcloud run deploy fastapi-tp --image=...:vN`

Cloud Build agarra la carpeta `api/`, la sube a un bucket temporal, levanta una VM efímera con Docker (que después se destruye), ejecuta cada paso del Dockerfile, y sube la imagen final a Artifact Registry. Tarda alrededor de un minuto. Una vez que la imagen está en el registry, `gcloud run deploy` le dice a Cloud Run que use esa nueva imagen, crea una revisión nueva e inmutable, y le manda el tráfico al container nuevo. Lo lindo es que no hay downtime: Cloud Run no apaga el container viejo hasta que el nuevo está respondiendo OK.

### 6.3 Versionado de imágenes y rollback

Cada deploy se realizó con un tag explícito (`v1`, `v2`, `v3`) en lugar del tag `:latest`, que sería el default. Esto agrega trabajo manual mínimo (acordarse de incrementar el número), pero a cambio provee información importante: si una versión nueva trae bugs, se puede volver a la anterior con un solo deploy, porque la imagen anterior sigue viva en Artifact Registry. A esto vimos que se le llama *rollback*.

Cloud Run también guarda un historial de revisiones del servicio. Eso permite, si se quisiera, dividir el tráfico entre revisiones (lo que se llama *canary deployment*: "mandá el 90% del tráfico a la versión vieja y el 10% a la nueva, así si algo se rompe afecta a poca gente"). Si bien este enfoque no fue implementado en el presente trabajo práctico, resulta relevante mencionarlo como una posible alternativa o extensión futura del sistema.

### 6.4 Variables de entorno y manejo de secretos

Para que la API y el DAG puedan conectarse a Postgres necesitan saber la dirección del servidor, el usuario y el password. En vez de dejar esos datos escritos en el código (lo cual sería un riesgo de seguridad y una incomodidad para cambiar el password después), los pasamos como variables de entorno: valores que la aplicación lee al arrancar y que viven afuera del repositorio.

Acá quedó una asimetría que vale la pena admitir: cada componente terminó leyendo las credenciales de un lugar distinto. La API, que corre en Cloud Run, usa una sola variable llamada `POSTGRES_URI` que contiene toda la string de conexión armada (host, puerto, usuario, password, nombre de base). Esa variable la inyectamos en Cloud Run al momento del deploy con el flag `--set-env-vars`. El DAG, que corre en la VM de Airflow, usa cuatro variables separadas (`RECOS_DB_HOST`, `RECOS_DB_USER`, `RECOS_DB_PASSWORD`, `RECOS_DB_NAME`) que dejamos en el `~/.bashrc` del usuario `pipeposse`. Las dos formas funcionan, pero idealmente habríamos unificado el formato. La razón de que estén distintas es histórica: cada componente lo armamos en momentos distintos del desarrollo y cada uno quedó con la convención que le había salido más natural.

En el repositorio dejamos también un archivo `.env.example` con los nombres de las variables y valores ficticios (por ejemplo `TU_PASSWORD` en lugar del password real). La idea es que cualquier persona que clone el proyecto pueda ver de un vistazo qué credenciales necesita configurar para levantarlo, sin que las credenciales reales aparezcan en el código.

Hay un detalle más, que es producto de un problema real con el que nos chocamos. La API tiene una línea al arrancar que normaliza la URI antes de pasársela a `psycopg2`. Resulta que existen dos formas de escribir esa URI: la "extendida" (`postgresql+psycopg2://...`), que algunas librerías esperan, y la "limpia" (`postgresql://...`), que es la que `psycopg2` entiende directamente. Si le pasás la extendida, falla al arrancar. Como en algún momento la variable había quedado seteada con el prefijo largo, en lugar de andar tocando la configuración de Cloud Run cada vez, agregamos una línea que reemplaza un prefijo por el otro. Es un parche más que una solución elegante, pero funciona y nos sacó del problema. La historia completa está en la sección 8.

### 6.5 Networking: la IP estática y el firewall

La VM de Airflow tiene una IP externa estática (`34.55.205.251`), reservada como recurso aparte en GCP. La promovimos de IP efímera (que cambia si la VM se reinicia) a estática (que queda fija para siempre) con un solo comando. Sin esto, cualquier reinicio podría cambiarle la IP y la URL que le pasamos al corrector quedaría rota.

La regla de firewall `allow-airflow-ui` permite tráfico entrante TCP en el puerto 8080 desde cualquier origen (`0.0.0.0/0`). Suena medio permisivo, pero es aceptable porque Airflow tiene su propia autenticación con usuario y password: aunque cualquiera llegue al puerto, sin login no entra.

La API en Cloud Run, por su parte, se expone a través de una URL HTTPS estable que provee la plataforma. 
---

## 7. Decisiones tomadas durante el desarrollo

### 7.1 Endpoints con path params en lugar de query params

La primera versión de la API tenía endpoints con la forma `/recommendations/{advertiser_id}?model=TopCTR`. Es decir, el modelo iba como query parameter (los que van después del signo de pregunta). Sin embargo, al revisar nuevamente la consigna se observó que el formato esperado era mediante path parameters (parte de la URL): `/recommendations/{advertiser_id}/{modelo}`.

Aunque la diferencia parece menor, conceptualmente no es equivalente. En el primer caso el modelo actúa como un parámetro opcional agregado a la URL, mientras que en el segundo forma parte explícita del recurso solicitado y pasa a ser obligatorio.

A partir de este cambio fue necesario reestructurar los endpoints y agregar validaciones explícitas sobre los modelos permitidos. De esta manera, cuando un usuario envía un modelo inexistente, la API responde con un 400 Bad Request y un mensaje descriptivo, evitando errores internos o respuestas ambiguas.

### 7.2 Migración del esquema de datos a un modelo normalizado

La primera versión del sistema utilizaba dos tablas separadas (`top_ctr` y `top_product`) con una columna TEXT que contenía la lista de los 20 productos serializada como literal Python. La API parseaba esa lista con `ast.literal_eval` para devolverla como JSON. Esa decisión simplificaba el código del DAG (un `to_sql(if_exists="replace")` por modelo) pero introducía dos problemas significativos:

1. **No permitía mantener histórico:** la estrategia de `if_exists="replace"` reemplaza la tabla entera en cada corrida del DAG, por lo que la base solo conservaba el último día.
2. **No era queryable:** filtrar, ordenar o contar productos individuales requería parsear strings en el cliente, lo cual es lento y frágil.

La versión final migra a un esquema normalizado (`recommendations` con una fila por producto) y al patrón de upsert con `ON CONFLICT`. El DAG ahora identifica filas existentes por su clave natural `(advertiser_id, model, product_id, date)` y las actualiza o inserta según corresponda. Las queries de la API se simplificaron: dejaron de necesitar parseo de strings y pasaron a ser SELECT convencionales.



#### Cómo funciona `ON CONFLICT`, con un ejemplo 

Vamos a verlo con la tabla `recommendations` en mano. Supongamos que ya tiene estas tres filas, resultado de una corrida del DAG el 2026-05-09:

| advertiser_id | model     | product_id | rank | score | date       |
|---------------|-----------|------------|------|-------|------------|
| ADV1          | top_ctr   | prod42     | 5    | 0.10  | 2026-05-09 |
| ADV1          | top_ctr   | prod88     | 6    | 0.09  | 2026-05-09 |
| ADV2          | top_ctr   | prodX      | 1    | 0.42  | 2026-05-09 |

A la tarde, nos damos cuenta de que el CSV de `ADV1` había llegado con un click faltante. Reprocesamos el día. El DAG vuelve a calcular y ahora `prod42` para `ADV1` tiene `rank=3, score=0.18`. Y descubrimos también un producto nuevo, `prod99`, que no estaba antes (`rank=12, score=0.05`).

Sin `ON CONFLICT`, esto sería un drama: el primer INSERT (el de `prod42`) chocaría con la fila vieja y la base devolvería un error de "duplicate key", abortando la transacción. Tendríamos que primero borrar lo viejo, después insertar lo nuevo, y rezar para que nadie consulte la API en el medio.

Con `ON CONFLICT`, en cambio, mandamos un único INSERT por fila y le decimos a Postgres qué hacer si choca:

```sql
-- Reemplazo: prod42 ya existía con rank=5, lo actualizamos a rank=3.
INSERT INTO recommendations (advertiser_id, model, product_id, rank, score, date)
VALUES ('ADV1', 'top_ctr', 'prod42', 3, 0.18, '2026-05-09')
ON CONFLICT (advertiser_id, model, product_id, date)
DO UPDATE SET rank = EXCLUDED.rank, score = EXCLUDED.score;

-- Inserción limpia: prod99 no existía, entra como fila nueva.
INSERT INTO recommendations (advertiser_id, model, product_id, rank, score, date)
VALUES ('ADV1', 'top_ctr', 'prod99', 12, 0.05, '2026-05-09')
ON CONFLICT (advertiser_id, model, product_id, date)
DO UPDATE SET rank = EXCLUDED.rank, score = EXCLUDED.score;
```

La tabla queda así:

| advertiser_id | model     | product_id | rank      | score      | date       | Qué pasó      |
|---------------|-----------|------------|-----------|------------|------------|---------------|
| ADV1          | top_ctr   | prod42     | **3**     | **0.18**   | 2026-05-09 | UPDATE        |
| ADV1          | top_ctr   | prod88     | 6         | 0.09       | 2026-05-09 | (sin cambios) |
| ADV2          | top_ctr   | prodX      | 1         | 0.42       | 2026-05-09 | (sin cambios) |
| **ADV1**      | **top_ctr** | **prod99** | **12**  | **0.05**   | **2026-05-09** | INSERT    |

Tres ideas clave de este ejemplo:

1. **La "firma" de cada fila** son las cuatro columnas que aparecen entre paréntesis después de `ON CONFLICT`: `(advertiser_id, model, product_id, date)`. Postgres las usa para decidir si lo que entra ya estaba o no.
2. **La pseudo-tabla `EXCLUDED`** representa la fila que estábamos intentando insertar. Cuando hay conflicto, accedemos a sus valores con `EXCLUDED.rank`, `EXCLUDED.score`, etc. para sobrescribir las columnas existentes.
3. **`prod88` quedó intacto.** `ON CONFLICT` no es un "DELETE + reinsert". Solo toca las filas que efectivamente colisionan; el resto del estado de la tabla queda como estaba. Eso es lo que nos permite acumular histórico de varios días en la misma tabla sin pisarnos.

#### Cómo llegamos a esta solución

La elección de `ON CONFLICT` no fue a priori, sino producto de iteración, inistencia y búsqueda de una solucíon más robusta y fructífera. La primera versión del DAG escribía a Postgres con `pandas.to_sql(..., if_exists="replace")`, que reemplazaba la tabla entera en cada corrida. No producía errores, pero tampoco preservaba histórico: el endpoint `/history/` devolvía siempre un único día porque cada corrida pisaba la anterior. Eso nos pareció un picardía, por lo que decidimos a último momento darlo una solución.

Identificada la limitación, evaluamos tres alternativas para "agregar sin pisar":

1. **`SELECT` previo + `UPDATE` o `INSERT`** según existiera la fila. Dos round-trips a Postgres por cada registro y posible *race condition* entre lectura y escritura. Una race condition es cuando dos procesos hacen algo “al mismo tiempo” y el resultado depende de quién llega primero.
2. **`DELETE WHERE date = ds` + `INSERT` limpio.** Funcional, pero abre una ventana en la que la tabla queda momentáneamente sin las filas del día procesado.
3. **`INSERT ... ON CONFLICT ... DO UPDATE`,** una única sentencia atómica que resuelve inserción y actualización sin ventanas de inconsistencia. Es el patrón estándar para *upsert* en cualquier motor relacional moderno. Fue una interesante solución, si no existe aplica INSERT, si existe, UPDATE.

La tercera opción era la más limpia. Al implementarla nos encontramos con el error `there is no unique or exclusion constraint matching the ON CONFLICT specification`. Ese mensaje resultó uno de los aprendizajes más valiosos del proyecto: `ON CONFLICT` no opera en abstracto, requiere un índice único físico sobre las columnas declaradas, y es Postgres quien lo utiliza como referencia para detectar el conflicto. Tras crear el índice `uq_recommendations_natural_key`, el DAG completó su ejecución correctamente. Costó un tiempo darnos cuenta de que la solución consistía en ponerle un index a la tabla-

La secuencia "elegir el patrón correcto → leer el error → entender qué espera el motor → proveerlo" sintetiza buena parte del valor pedagógico de la migración: confirmó que los principios del modelo relacional no son convenciones arbitrarias, sino requisitos concretos que sostienen las garantías de consistencia que terminamos aprovechando en el resto del sistema.

### 7.3 Acceso a Postgres con `psycopg2` directo en API y DAG

Tanto la API como el DAG se conectan a Postgres usando `psycopg2` directamente, sin ORM ni capas adicionales. Las operaciones del proyecto son simples (lecturas con SELECT en la API, un INSERT bulk con upsert en el DAG), por lo que un ORM solo agregaría complejidad sin valor proporcional.

Como anécdota: la primera versión del DAG escribía a Postgres con `pandas.to_sql(..., if_exists='replace')`, lo cual implicaba arrastrar SQLAlchemy como dependencia indirecta y reemplazar las tablas enteras en cada corrida. Cuando migramos al esquema normalizado tuvimos que reescribir esa parte porque `to_sql` no soporta `ON CONFLICT`, y necesitábamos upsert real para acumular histórico. La nueva implementación usa `psycopg2.extras.execute_values`, que permite hacer un INSERT bulk con `ON CONFLICT` en una sola query, y como bonus eliminó SQLAlchemy del lado del DAG. Hoy el sistema es consistente: el mismo driver en los dos componentes.

### 7.4 Cloud Run en lugar de una VM dedicada para la API

Para desplegar la API teníamos dos caminos: levantar otra VM aparte y correrla ahí, o usar Cloud Run. Elegimos Cloud Run y la diferencia de comodidad fue grande. Una VM hay que prenderla, mantenerla, configurarle un dominio, conseguir un certificado para HTTPS y dejar todo eso funcionando. Cloud Run hace todo eso por vos: vos le dás la imagen Docker y el servicio te devuelve una URL `https://...` que ya está lista para recibir requests, sin que hayamos tocado un certificado en ningún momento.

Otra ventaja concreta es que Cloud Run "escala solo". Si nadie está usando la API, no hay ningún contenedor corriendo y no pagás nada. Si llegan muchos requests al mismo tiempo, levanta más contenedores automáticamente. En una VM tendríamos que dimensionarla a ojo y, o nos quedaba chica si entraba un pico, o nos quedaba grande pagando de más.


### 7.5 Versionar imágenes Docker con vN en vez de :latest

Las imágenes Docker las subimos a Artifact Registry con tags explícitos (`v1`, `v2`, `v3`) en lugar de pisar siempre `:latest`, que sería el default. Esto agrega trabajo manual mínimo, pero a cambio nos da la posibilidad inmediata para volver para atrás.

---

## 8. Problemas encontrados y resolución

### 8.1 La connection string que la API no entendía

**Síntoma:** después de un deploy, los endpoints empezaron a tirar `500 Internal Server Error`. `/health` respondía OK, pero cualquier endpoint que tocara la base explotaba. Encontrar la causa nos llevó un rato hasta que fuimos a leer los logs en Cloud Run.

El mensaje exacto era:

```
psycopg2.ProgrammingError: invalid dsn: missing "=" after
"postgresql+psycopg2://..." in connection info string
```

Resulta que la URI seteada en la variable de entorno tenía un prefijo extendido (`postgresql+psycopg2://`) que algunas librerías como SQLAlchemy aceptan, pero `psycopg2` directo no. Cuando `psycopg2` ve un string que no arranca con `postgresql://` limpio, intenta interpretarlo como un DSN de tipo "clave=valor" separado por espacios, y al no encontrar el `=` se rompe.

La solución fue agregar tres líneas al inicio de `main.py` que normalizan la URI: si arranca con `postgresql+psycopg2://`, lo reemplazamos por `postgresql://` limpio. Así el código se vuelve robusto frente a las dos formas de escribir la URI, y no tuvimos que tocar la variable de entorno en Cloud Run, lo cual evita la fricción de redeployar configuración.

### 8.2 El scheduler de Airflow huérfano sin variables de entorno

**Síntoma:** durante varios días el DAG no procesó datos nuevos. La API seguía respondiendo (porque las tablas de la base seguían teniendo los datos viejos) pero la fecha más reciente que devolvía empezó a ser cada vez más vieja. Cuando lo notamos, fuimos a investigar.

Lo primero que probamos fue listar los DAGs registrados en Airflow:

```
$ airflow dags list
Error: Failed to load all files. For details, run
`airflow dags list-import-errors`
No data found
```

Corriendo `list-import-errors` apareció el problema:

```
RuntimeError: Falta la variable de entorno POSTGRES_URI.
Definirla en ~/.bashrc del usuario que corre Airflow.
```

La variable estaba en `.bashrc`, sí. El tema es que `.bashrc` solo lo lee una shell interactiva (cuando hacés login y aparece el prompt). El proceso del scheduler había arrancado en algún momento desde una shell que tenía la variable, pero después esa shell se cerró y el scheduler quedó huérfano. Un proceso huérfano en Linux es uno cuyo padre murió: el sistema operativo lo re-asigna al proceso `init` (PID 1), por eso se reconoce viendo `PPID=1`. Los workers que Airflow lanza desde ese scheduler huérfano no heredan más la variable de la shell original que ya no existe.

La solución fue matar todos los procesos viejos, abrir una shell limpia que cargara `.bashrc` (haciendo `source ~/.bashrc`), y volver a arrancar el scheduler y el webserver con `nohup`. Para una solución más definitiva habría que crear unidades systemd con la variable definida en el archivo `.service`, pero por tiempo lo dejamos para una mejora futura.

Una vez más, el orden de como se ejecutan los procesos terminó siendo crítico. 

### 8.3 La memoria del proyecto que tenía un nombre de DAG equivocado

Mientras debugueábamos el problema anterior, intentamos verificar los runs históricos del DAG con:

```
airflow dags list-runs -d adtech_recos
```

Y nos devolvía vacío. Pensábamos que el DAG estaba sin runs, pero el problema era otro: en el archivo del DAG el `dag_id` estaba puesto como `adtech_pipeline`, no `adtech_recos`. Habíamos cambiado el nombre en algún punto del desarrollo y no lo registramos.

Es un problema chiquito pero ilustra algo importante: los nombres son contratos. Si el `dag_id` en el código no matchea con lo que esperan los comandos o la documentación, todo se rompe. La lección fue revisar siempre el código fuente como fuente de verdad, no las notas viejas.

### 8.4 ON CONFLICT contra una tabla sin índice único


**Síntoma:** una vez migrado al modelo normalizado, el DAG empezó a fallar en el último task con un error específico:

```
psycopg2.errors.InvalidColumnReference:
  there is no unique or exclusion constraint matching the
  ON CONFLICT specification
```

El código del DAG hacía `INSERT ... ON CONFLICT (advertiser_id, model, product_id, date) DO UPDATE`, pero el índice único correspondiente no existía en la tabla `recommendations`. El `schema.sql` lo definía, pero ese script nunca se había ejecutado completo en la base.

La regla de PostgreSQL es estricta: para que `ON CONFLICT (cols...)` funcione, tiene que existir un constraint único o un índice único sobre exactamente esas columnas. La solución fue crear el índice a mano con un solo comando:

```sql
CREATE UNIQUE INDEX uq_recommendations_natural_key
    ON recommendations (advertiser_id, model, product_id, date);
```

Antes de crearlo verificamos que no hubiera filas duplicadas que pudieran impedir la creación del índice (la tabla estaba vacía porque el DAG fallaba antes de insertar). El `schema.sql` del repositorio se actualizó para que el índice forme parte de la fuente de verdad del schema y futuras inicializaciones lo creen automáticamente.

### 8.5 La API leyendo tablas obsoletas tras la migración

**Síntoma:** después de la migración al esquema normalizado, los endpoints `/recommendations/...` devolvían fechas viejas (`2026-04-18`) en lugar de la del último DAG run (`2026-04-19`). El DAG corría correctamente y poblaba `recommendations`, pero la API parecía no notar el cambio.

Investigando descubrimos que existía una "doble vida" temporal: las tablas viejas (`top_ctr`, `top_product`) seguían poblándose por algún artefacto histórico, y la API seguía consultándolas porque su código no había sido actualizado. Confirmamos la sospecha haciendo `SELECT MAX(date) FROM top_ctr` (devolvió `2026-04-18`, idéntico a la fecha que la API reportaba) y comparándolo con `SELECT MAX(date) FROM recommendations` (devolvió `2026-04-19`).

La solución fue reescribir el `main.py` de la API para leer únicamente de `recommendations`, rebuildear la imagen Docker (`fastapi-tp:v3`), redeployarla a Cloud Run, y dropear las tablas viejas (`DROP TABLE top_ctr; DROP TABLE top_product;`). Tras este último paso confirmamos que el endpoint `/recommendations/<adv>/TopCTR` devolvía `date: 2026-04-19` y que `/stats/` recalculaba el Jaccard sobre los datos nuevos.

---

## 9. Limitaciones actuales y trabajo futuro

Si bien el sistema cumple con los objetivos planteados, existen diversas áreas susceptibles de mejora sobre las cuales hubiera sido deseable profundizar. Estas limitaciones y oportunidades de evolución se documentan explícitamente a continuación, con el objetivo de reflejar de manera transparente el estado actual de la implementación.

- **TopCTR sin filtro de impresiones mínimas.** El modelo no descarta productos con pocas impresiones. Un producto con 2 impresiones y 2 clicks tiene CTR = 1.0 y puede ganar el ranking. La mejora directa sería agregar un filtro tipo `impressions_count >= N` (por ejemplo `N = 10`) antes de calcular el ranking, o aplicar un suavizado bayesiano (Wilson score, smoothing tipo Laplace) para penalizar las observaciones de bajo volumen.

- **Configuración de conexión asimétrica entre DAG y API.** Ambos usan `psycopg2`, pero el DAG lee las credenciales de cuatro variables de entorno separadas (`RECOS_DB_HOST`, `RECOS_DB_USER`, `RECOS_DB_PASSWORD`, `RECOS_DB_NAME`) mientras que la API consume una única `POSTGRES_URI`. Una mejora menor sería unificar el formato de configuración entre ambos.

El DAG (que corre en la VM de Airflow) tiene 4 variables de entorno separadas, una por cada dato:

RECOS_DB_HOST=34.46.239.72
RECOS_DB_USER=tp-user
RECOS_DB_PASSWORD=*****
RECOS_DB_NAME=recos_db

Y a la hora de conectar:

psycopg2.connect(host=os.environ["RECOS_DB_HOST"],
                 user=os.environ["RECOS_DB_USER"],
                 password=os.environ["RECOS_DB_PASSWORD"],
                 dbname=os.environ["RECOS_DB_NAME"])
                 


La API (que corre en Cloud Run) tiene una sola variable, con todo armado en una URL:
POSTGRES_URI=postgresql://tp-user:password@34.46.239.72:5432/recos_db


Por qué es una "asimetría"
Las dos formas funcionan, llegan a la misma base, usan el mismo driver. Pero son dos maneras distintas de decir lo mismo. Si mañana cambia el password de la base, hay que tocarlo en dos lugares con dos formatos distintos: en la VM editás 1 de las 4 variables de entorno; en Cloud Run reemplazás la URL entera con el password nuevo URL-encoded en el medio.



- **Tests automatizados.** La API y el DAG no tienen tests unitarios ni de integración. En un sistema de producción real escribiríamos tests con `pytest` para cada endpoint y para el flujo de transformación. Los validamos manualmente con `curl`, pero no es lo mismo. No conocemos mucho de este tema, pero sabemos que existe y sumaría mucho en calidad.

- **Conexión a Cloud SQL más segura.** La API se conecta a la base por IP pública. Cloud Run permite conectarse via Cloud SQL Auth Proxy, lo que evita exponer la base por internet.

- **Monitoreo y alertas.** No tenemos alertas configuradas para fallos del DAG ni del servicio. Cloud Monitoring permitiría detectar regresiones automáticamente sin que tengamos que mirar logs.

- **Autenticación de la API.** Los endpoints son públicos. Para un escenario real habría que poner API keys, OAuth o IAM.

-- **Auto-arranque del scheduler y webserver de Airflow.** Los dos servicios los lanzamos a mano desde la consola con `nohup` (un comando que deja un programa corriendo en segundo plano aunque cerremos la sesión SSH). El problema es que si la VM se reinicia por cualquier motivo —un mantenimiento de Google, un apagado accidental— el scheduler y el webserver no se vuelven a prender solos: hay que entrar por SSH y levantarlos manualmente. La mejora sería configurarlos como servicios del sistema operativo (lo que Linux llama `systemd`), de modo que arranquen automáticamente cuando se prende la VM. No lo implementamos por tiempo, pero es un cambio relativamente directo cuando se necesite. 

---

## 10. Conclusiones

Este TP recorrió el ciclo completo de un sistema de datos en producción: desde archivos crudos almacenados en un bucket hasta una API HTTPS pública, pasando por orquestación, base de datos relacional, containers, deploy versionado y debugging de errores reales en producción.

Los servicios administrados de GCP utilizados (Cloud Storage, Compute Engine, Cloud SQL, Artifact Registry, Cloud Build y Cloud Run) permitieron resolver la infraestructura sin necesidad de administrar servidores propios. Esto resultó clave para poder concentrar el trabajo en la lógica del pipeline, el procesamiento de datos y el desarrollo de la aplicación, en lugar de invertir tiempo en la configuración manual de infraestructura.

Los dos modelos implementados, TopCTR y TopProduct, mostraron comportamientos significativamente distintos al analizar su similaridad mediante el índice de Jaccard. Los valores obtenidos, entre 0.081 y 0.333, indicaron que ambos modelos capturan señales diferentes dentro de los datos. Esto justificó mantener ambos enfoques dentro del sistema, ya que un nivel de similaridad mucho mayor habría sugerido redundancia entre ellos.

Una de las experiencias más enriquecedoras del proyecto fue **migrar el modelo de datos en producción**. La primera versión del sistema funcionaba pero arrastraba un anti-patrón (listas serializadas en columnas TEXT) que limitaba severamente lo que se podía hacer con los datos. Pasar al esquema normalizado nos obligó a coordinar cambios coherentes en la base, en el DAG y en la API, validando cada paso con queries de verificación. Esa secuencia (`CREATE INDEX` → reescritura de queries → rebuild de la imagen → redeploy → drop de tablas obsoletas) es un microcosmos de cómo se hacen las migraciones reales.

Más allá del resultado funcional, uno de los aspectos más valiosos del trabajo fue el aprendizaje práctico asociado al desarrollo y operación de sistemas reales. El proyecto permitió trabajar con logs centralizados, interpretar stack traces, comprender problemas asociados a procesos huérfanos y variables de entorno, entender cómo Cloud Build genera capas Docker reutilizables y profundizar en conceptos como servicios stateless. Muchos de estos temas suelen abordarse de manera teórica, pero en este caso pudieron experimentarse directamente a través de la resolución de errores reales durante el desarrollo y despliegue del sistema.

El resultado final es un proyecto que puede comprenderse integralmente, desde la generación de datos hasta el serving de recomendaciones, incluyendo tanto sus fortalezas como sus limitaciones.

---

## 11. Anexos

### A. URLs públicas y credenciales para evaluación

| Recurso | Acceso |
|---|---|
| API REST (Cloud Run) | https://fastapi-tp-xbo6kajhza-uc.a.run.app |
| Documentación interactiva (Swagger) | https://fastapi-tp-xbo6kajhza-uc.a.run.app/docs |
| Airflow UI | http://34.55.205.251:8080 |
| Repositorio Git | https://github.com/pipeposse/tp-final-adtech |

**Credenciales de Airflow para evaluación (rol Viewer, solo lectura):**

- Usuario: `profesor`
- Password: `profesor`

### B. Comandos de despliegue

**Construcción de imagen Docker via Cloud Build:**

```bash
gcloud builds submit api/ \
  --tag=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:vN \
  --project=tp-final-adtech-493922
```

**Despliegue a Cloud Run:**

```bash
gcloud run deploy fastapi-tp \
  --image=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:vN \
  --region=us-central1 --project=tp-final-adtech-493922 \
  --set-env-vars 'POSTGRES_URI=postgresql+psycopg2://USER:PASSWORD@HOST:5432/recos_db'
```

**Reserva de IP estática para la VM de Airflow:**

```bash
gcloud compute addresses create diegue-pipe-belu-airflow-ip \
  --addresses=34.55.205.251 \
  --region=us-central1 --project=tp-final-adtech-493922
```

**Creación del usuario `profesor` en Airflow:**

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
  "date": "2026-05-08",
  "recommendations": ["0639y2", "1mlwn4", "2bx2zk", "..."]
}
```

Fragmento de respuesta de `/stats/`, sección de coincidencia entre modelos:

```json
"coincidencia_entre_modelos": [
  {
    "advertiser_id": "OAGTYWN8WFC997VLDJH7",
    "productos_en_comun": 10,
    "jaccard": 0.333
  },
  ...
]
```

### D. Ejemplos de consulta a la API

Para que el evaluador pueda probar la API directamente desde un navegador, dejamos URLs listas para pegar. Como advertiser de prueba usamos `OAGTYWN8WFC997VLDJH7`, que es uno real del dataset y es además el que tiene mayor similaridad de Jaccard entre los dos modelos (0.333), por lo que sirve para ver el sistema en plenitud.

**Health check (verifica que la API esté viva)**

```
https://fastapi-tp-xbo6kajhza-uc.a.run.app/health
```

Respuesta esperada: `{"status":"ok"}`. No toca la base, solo confirma que el container está respondiendo.

**Ver recomendaciones por TopCTR para un advertiser**

```
https://fastapi-tp-xbo6kajhza-uc.a.run.app/recommendations/OAGTYWN8WFC997VLDJH7/TopCTR
```

Devuelve un JSON con el `advertiser_id`, el modelo (`TopCTR`), la fecha de la última corrida del DAG, y la lista de los 20 productos recomendados ordenados por click-through ratio.

**Ver recomendaciones por TopProduct para el mismo advertiser**

```
https://fastapi-tp-xbo6kajhza-uc.a.run.app/recommendations/OAGTYWN8WFC997VLDJH7/TopProduct
```

Mismo formato pero con los 20 productos ordenados por cantidad de visualizaciones. Los productos van a ser distintos a los de TopCTR (es la confirmación práctica de que los modelos no son redundantes).

**Ver el historial de un advertiser**

```
https://fastapi-tp-xbo6kajhza-uc.a.run.app/history/OAGTYWN8WFC997VLDJH7/
```

Devuelve los últimos 7 días de recomendaciones para ambos modelos. Si la última corrida del DAG es más vieja que el cutoff, devuelve 404 (por diseño).

**Ver estadísticas agregadas y análisis Jaccard**

```
https://fastapi-tp-xbo6kajhza-uc.a.run.app/stats/
```

Devuelve un JSON con cuatro secciones: cantidad de consultas por día, cantidad de advertisers procesados por modelo, y una lista ordenada por similaridad Jaccard donde cada fila muestra cuántos productos comparten los modelos TopCTR y TopProduct para cada advertiser.

**Probar un caso de error — modelo inválido**

```
https://fastapi-tp-xbo6kajhza-uc.a.run.app/recommendations/OAGTYWN8WFC997VLDJH7/ModeloInventado
```

Respuesta esperada: HTTP 400 con el mensaje `Modelo invalido: 'ModeloInventado'. Opciones: ['TopCTR', 'TopProduct']`. Es la confirmación de que la API valida los inputs en lugar de explotar.

**Probar un caso de error — advertiser inexistente**

```
https://fastapi-tp-xbo6kajhza-uc.a.run.app/recommendations/ADV_INEXISTENTE/TopCTR
```

Respuesta esperada: HTTP 404 con el mensaje `No hay recomendaciones para advertiser 'ADV_INEXISTENTE' con modelo 'TopCTR'`.

**Otros `advertiser_id` reales para probar**

Cualquiera de los siguientes funciona y devuelve resultados:

- `OAGTYWN8WFC997VLDJH7` (Jaccard 0.333, el más alto)
- `AK81O7W3KGPEN8LABG2N` (Jaccard 0.250)
- `LW045DVYSGRD75TK6U54` (Jaccard 0.250)
- `5E325T5HYL61QSABVR5V` (Jaccard 0.212)
- `EN1SA43DTN2LIR8DEW5S` (Jaccard 0.212)
- `M0LU6DCI1WILGQBZ6808` (Jaccard 0.212)
- `K6Z0X85ZUY0TSF4RCG5J` (Jaccard 0.111)

**Forma alternativa: documentación interactiva**

En lugar de armar las URLs a mano, recomendamos abrir [https://fastapi-tp-xbo6kajhza-uc.a.run.app/docs](https://fastapi-tp-xbo6kajhza-uc.a.run.app/docs) y usar la interfaz Swagger. Cada endpoint tiene un botón "Try it out" que permite escribir los parámetros en formularios y ejecutar la consulta sin tener que pensar la URL. La verdad, muy cómodo para testear.
