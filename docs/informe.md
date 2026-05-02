**Universidad de San Andrés**

Programación Avanzada

**Trabajo Práctico Final**

Pipeline de Recomendaciones AdTech

*Autores*

Felipe Posse

Diego Sanguinetti

Belén Candela Lozada Montanari

*Profesores*

Agustín Mosteiro · Matías Dinota

Fecha de entrega: 9 de mayo de 2026

Índice

1\. De qué va este TP

2\. Qué nos propusimos y hasta dónde llegamos

3\. La arquitectura, vista desde arriba

4\. Las piezas, una por una

5\. Los dos modelos de recomendación

6\. Cómo lo desplegamos

7\. Las decisiones que tomamos en el camino

8\. Lo que se nos rompió y cómo salimos

9\. Lo que quedó para mejorar

10\. Conclusiones

11\. Anexos

1\. De qué va este TP

La idea, en una frase: armamos un sistema que mira datos de impresiones y clicks de publicidad online, y le dice a cada anunciante (le decimos advertiser de acá en adelante) cuáles son sus 20 productos más interesantes según dos formas distintas de medirlo. Todo eso corre solo, todos los días, sin que tengamos que hacer nada manual.

Por debajo, el sistema tiene cinco piezas principales: un bucket en la nube donde caen los archivos crudos (un bucket es básicamente una carpeta gigante online), una máquina virtual que corre Apache Airflow (que es la \"agenda\" que decide cuándo correr cada tarea), una base de datos PostgreSQL administrada por Google donde se guardan los resultados, una API web que devuelve esos resultados cuando alguien la consulta, y todo desplegado en Google Cloud Platform.

Implementamos los dos modelos pedidos: TopCTR, que ordena los productos por click-through ratio (cuántos clicks generan respecto a las veces que se mostraron), y TopProduct, que los ordena por cantidad pura de visualizaciones. Como bonus, calculamos cuánto se parecen las recomendaciones de los dos modelos usando la similaridad de Jaccard, una métrica clásica para comparar conjuntos. Más adelante explicamos qué es y cómo se interpreta.

Toda la infraestructura está en Google Cloud y se administra desde la consola web o con el comando gcloud. La API la empaquetamos en una imagen Docker (un \"tupper\" que tiene adentro la app y todo lo que necesita), la subimos a un registry de imágenes, y la deployamos en Cloud Run, que es un servicio que corre containers Docker detrás de una URL HTTPS sin que tengamos que mantener un servidor.

2\. Qué nos propusimos y hasta dónde llegamos

2.1 De qué va el problema

La industria AdTech (que es la publicidad digital programática) genera diariamente cantidades enormes de eventos. Cada vez que alguien ve un anuncio se registra una impresión, y si hace click, se registra un click. Los anunciantes necesitan información digerida sobre qué productos están funcionando bien para decidir dónde poner la plata, qué creatividades empujar, y a quién apuntar.

Hacer este tipo de pipeline mete en una sola cosa varios temas que normalmente se enseñan por separado: traer los datos (ingesta), procesarlos en una pasada nocturna (eso se llama batch processing, que es \"todo junto a una hora fija\" en vez de \"a medida que va llegando\"), guardarlos en una base, y poner una API que los exponga al mundo. Lo lindo es que el TP integra todo eso. Lo desafiante es que cada componente se puede romper de formas distintas, y cuando algo falla en un sistema con cinco piezas, hay que saber dónde mirar.

2.2 Lo que nos planteamos al arrancar

-   Diseñar y desplegar un pipeline completo en Google Cloud, desde el archivo crudo hasta una API pública.

-   Orquestar el procesamiento diario con Apache Airflow, de manera que cada paso quede registrado y, si falla, se pueda reintentar sin tener que hacer todo de cero.

-   Implementar los dos modelos pedidos (TopCTR y TopProduct) y, ya que estábamos, agregar un análisis de cuánto se parecen entre ellos.

-   Construir una API REST en FastAPI que cumpla los endpoints de la consigna, valide los inputs (o sea, que rechace cosas raras antes de llegar a la base) y use códigos HTTP correctos (200 para todo bien, 400 si te equivocás vos, 404 si no existe, 500 si nos rompimos nosotros).

-   Empaquetar la API en Docker y desplegarla en Cloud Run con tags versionados. ¿Por qué versionados? Para poder volver atrás si una versión nueva trae bugs.

2.3 Hasta dónde llegamos

El sistema procesa tres datasets de entrada por día: active\_advertisers (los anunciantes que están \"vivos\"), ads\_views (las impresiones y los clicks del día), y product\_views (las visualizaciones de productos en el sitio). La salida son dos tablas en la base, top\_ctr y top\_product, con las veinte mejores recomendaciones por advertiser y por modelo. La API expone consultas individuales, históricas y de estadísticas.

Hay decisiones que dejamos a medio camino y que documentamos en la sección 9 como \"lo que quedó para mejorar\". Las más importantes: el DAG sobreescribe las tablas en cada corrida (lo cual significa que no guardamos historia real), TopCTR no descarta productos con muy pocas impresiones (que pueden tener un CTR engañosamente alto por la muestra chica), no tenemos tests automatizados, y el scheduler de Airflow corre con un truco rústico (nohup) en lugar de un servicio prolijo de Linux. Las dejamos así por tiempo, no por desconocimiento.

3\. La arquitectura, vista desde arriba

3.1 La foto del sistema

Decidimos partir el sistema en cuatro capas con responsabilidades bien separadas. ¿Por qué? Porque cuando algo no anda, queremos saber rápido en qué pieza buscar. Si todo está mezclado, debugear es una pesadilla. Quedó así:

GCS bucket (los CSVs crudos viven aca, en gs://tp-final-adtech/raw/)

\|

v

Compute Engine VM (e2-small, una maquina virtual chica en us-central1-a)

\|\-- Apache Airflow (LocalExecutor, que corre todo en la misma VM)

\| \|\-- scheduler (el que decide cuando dispara el DAG)

\| \|\-- webserver (la UI web en el puerto 8080)

\| \\\-- DAG adtech\_pipeline (corre todos los dias)

\| \|\-- FiltrarDatos

\| \|\-- top\_ctr

\| \|\-- top\_product

\| \\\-- DBWriting

\\\-- Lee los CSVs locales de /home/pipeposse/trabajo\_practico/

\|

v

Cloud SQL (PostgreSQL 15 administrado por Google)

\|\-- airflow\_db (donde Airflow guarda su propia metadata)

\\\-- recos\_db

\|\-- top\_ctr (resultado del ultimo run del DAG)

\|\-- top\_product (resultado del ultimo run del DAG)

\\\-- api\_logs (registro de cada consulta a la API)

\^

\|

Cloud Run (la API FastAPI, dentro de una imagen Docker fastapi-tp:vN)

\|

v

Cliente HTTP (el corrector, nosotros, o cualquiera con la URL)

3.2 El recorrido del dato, de punta a punta

Los CSVs crudos viven en el bucket gs://tp-final-adtech/raw/. Importante: el DAG no los descarga directamente desde ahí. Los bajamos manualmente a la VM con gsutil cp (es el comando para copiar archivos entre tu máquina y un bucket) y los dejamos en /home/pipeposse/trabajo\_practico/. El DAG lee de esa carpeta local. Es una limitación reconocida que documentamos en la sección 9.

Cada día a las 00:00 UTC, el scheduler de Airflow dispara el DAG adtech\_pipeline con la fecha lógica correspondiente (que en Airflow se llama ds, date string, y viene en formato \"YYYY-MM-DD\"). Las cuatro tareas se ejecutan en este orden: primero FiltrarDatos lee los CSVs del día y se queda solo con los advertisers que están en active\_advertisers.csv, dejando los archivos filtrados en /tmp. Después top\_ctr y top\_product corren en paralelo, cada uno calcula su ranking de top 20 productos por advertiser. Al final, DBWriting agarra los dos rankings y los escribe a Postgres.

Acá hay un detalle que vale conocer: el código del DAG usa pandas.to\_sql con la opción if\_exists=\"replace\" para escribir las tablas. ¿Qué significa eso? Que cada corrida del DAG borra la tabla entera y la vuelve a crear con el resultado del día. La base no acumula histórico, solo guarda lo del último run. Esto tiene una consecuencia importante en el endpoint /history/ de la API, que vamos a explicar en la sección 4.

Por otro lado, totalmente desacoplado, el servicio FastAPI en Cloud Run consulta esas tablas cuando recibe un request HTTP. Cada vez que devuelve una recomendación, registra la consulta en una tabla auxiliar llamada api\_logs. Esa tabla la usamos en el endpoint /stats/ para mostrar uso de la API. La separación entre el batch (que procesa una vez al día) y el servicio HTTP (que está disponible 24x7) es muy típica en sistemas reales: nos permite escalar y deployar cada una por su lado.

4\. Las piezas, una por una

4.1 Cloud Storage: dónde caen los archivos

Los CSVs crudos viven en un bucket de Google Cloud Storage (GCS). Un bucket es como una carpeta gigante en la nube; pagás por GB guardado y por GB descargado. Es muchísimo más barato que meter todo en una base, y para datos no estructurados como CSVs es lo lógico.

Los archivos siguen un patrón de nombres por fecha:

active\_advertisers.csv (la lista de anunciantes vivos, no cambia por dia)

ads\_views\_YYYY-MM-DD.csv (impresiones y clicks del dia)

product\_views\_YYYY-MM-DD.csv (visualizaciones de productos del dia)

Elegimos GCS por sobre meter todo en Cloud SQL desde el principio porque es más barato, escala mejor, y queda desacoplado del resto. Si nos equivocamos en el procesamiento, podemos rebatear los archivos sin tocar la base.

4.2 Apache Airflow: el orquestador

Airflow corre sobre una máquina virtual e2-small (una VM chiquita) en us-central1-a de Compute Engine. ¿Qué es un orquestador? Es un programa que decide qué tarea correr, en qué orden, y qué hacer si una falla. Sin un orquestador, terminás con scripts pegados con cron que se rompen y no te enterás.

Acá tomamos una decisión importante. Google ofrece una versión administrada de Airflow que se llama Cloud Composer, que sería lo más prolijo. Pero tiene un costo fijo mensual bastante alto. Para un TP no nos cerraban los números, así que fuimos por la VM con Airflow instalado a mano.

¿Qué precio pagamos por esa decisión? Que tenemos que mantenerla nosotros. Si la VM se reinicia, hay que volver a levantar el scheduler. Si se rompe la base interna de Airflow, hay que arreglarla. Para un proyecto en producción real, Composer probablemente justificaría su costo en horas ahorradas. Para nosotros, no.

La instalación es directa: hay un entorno virtual de Python (un venv, que es una carpeta aislada donde pip instala las librerías solo para ese proyecto, sin chocar con otras cosas del sistema) en /home/pipeposse/airflow\_venv. La metadata interna de Airflow (qué DAGs hay, qué runs corrieron, los logs de cada tarea) la guarda en una base llamada airflow\_db dentro del mismo Cloud SQL del proyecto.

El DAG se llama adtech\_pipeline. Vale aclararlo porque durante el desarrollo lo llamamos adtech\_recos por un rato y después lo cambiamos. Si ven referencias viejas a adtech\_recos en notas internas, son errores nuestros. El nombre vivo es adtech\_pipeline.

Las cuatro tareas con sus dependencias quedan así: FiltrarDatos primero, después top\_ctr y top\_product en paralelo, y al final DBWriting que junta los resultados. La fecha que procesa cada run viene en el contexto de Airflow como ds, y el código usa esa fecha para construir el nombre de los archivos a leer (por ejemplo ads\_views\_2026-04-19.csv).

La interfaz web está en el puerto 8080 de la VM. Para que el corrector pueda entrar sin riesgo de tocar nada, le creamos un usuario aparte con rol Viewer (solo lectura). Ve los DAGs y los runs pero no puede triggear, pausar, ni editar.

4.3 Cloud SQL: la base de datos

Cloud SQL es PostgreSQL administrado por Google. En lugar de instalar Postgres en una máquina y mantenerlo a mano, Google te lo entrega: con backups, parches de seguridad y monitoreo incluidos.

Una sola instancia de Cloud SQL puede contener varias bases lógicas. Nosotros tenemos dos: airflow\_db con la metadata de Airflow, y recos\_db con los datos del proyecto. ¿Por qué separadas? Si las mezcláramos, una migración interna de Airflow podría tocar las tablas del TP, y los backups específicos serían más complicados.

Dentro de recos\_db tenemos tres tablas:

top\_ctr (advertiser\_id TEXT, top\_products TEXT, date TEXT)

top\_product (advertiser\_id TEXT, top\_products TEXT, date TEXT)

api\_logs (id SERIAL, advertiser\_id TEXT, model TEXT,

date TEXT, timestamp TIMESTAMP DEFAULT NOW())

La columna top\_products guarda la lista de los veinte productos como un literal de Python serializado a texto. O sea, en la base hay un string que dice algo como \"\[\'p1\', \'p2\', \'p3\', \...\]\". Esto fue una decisión heredada de cómo arrancamos el código: el DAG genera la lista con pandas, la serializa a CSV y la escribe a la base con to\_sql. La API después usa la función ast.literal\_eval para volver a convertirla en una lista real y devolverla como array JSON al cliente.

Acá está el detalle más importante que ya mencionamos: las tablas top\_ctr y top\_product se reescriben enteras en cada run del DAG. La razón es que el código usa if\_exists=\"replace\". Esto significa que la base nunca tiene más de un día de datos. Es una limitación que afecta directamente al endpoint /history/ de la API, descrita en la sección 9.

La tabla api\_logs la crea la API automáticamente al arrancar (en una función de inicialización que se llama lifespan, y que corre una sola vez cuando el container arranca). Registra cada consulta exitosa de recomendaciones, lo cual alimenta el endpoint /stats/.

4.4 La API FastAPI corriendo en Cloud Run

La API la armamos con FastAPI 0.115. ¿Qué es FastAPI? Es un framework de Python para hacer APIs web; vos escribís funciones de Python normales, las decorás con \@app.get(\...), y FastAPI las convierte en endpoints HTTP. Como bonus, te genera automáticamente la documentación interactiva en /docs.

Para servir las peticiones por HTTP, FastAPI necesita un servidor (no es un servidor por sí mismo, es solo el framework). Usamos Uvicorn en su versión \"standard\", que viene con dependencias adicionales que mejoran la performance.

La API se conecta a Postgres con psycopg2-binary directo, sin capas intermedias de abstracción. ¿Qué es psycopg2? Es el driver de Postgres para Python, lo que permite hablar con la base. Le decimos directo porque no usamos un ORM (un Object Relational Mapper, que es una capa que te permite escribir SQL como si fueran objetos de Python). Las queries de la API son simples (tres SELECT y un INSERT) y queríamos que el código sea fácil de leer.

Vale aclarar algo importante para honestidad técnica: el DAG sí usa SQLAlchemy (un ORM popular en Python) indirectamente, a través de pandas.to\_sql, que requiere un engine de SQLAlchemy para escribir. Es una asimetría heredada de cómo arrancamos el proyecto. Una mejora pendiente sería unificar ambos componentes en psycopg2 directo, pero por tiempo lo dejamos así.

El servicio se empaqueta como imagen Docker basada en python:3.12-slim (la versión \"chica\" de la imagen oficial de Python, \~50 MB en lugar de \~900 MB). Cloud Build (un servicio de Google que hace builds de Docker en la nube) se encarga de armar la imagen y subirla a Artifact Registry (que es como Docker Hub privado de Google) con un tag versionado: v1, v2, v3, v4. Cloud Run consume esa imagen y la sirve detrás de una URL HTTPS estable.

Los endpoints que expone son cinco:

  -------------------------------------------- ------------ ---------------------------------------------------------------------------
  **Endpoint**                                 **Método**   **Qué hace**
  /                                            GET          Devuelve {status: ok, service: \...}. Útil como ping.
  /health                                      GET          Health check sin tocar la base. Devuelve {status: ok}.
  /recommendations/{advertiser\_id}/{modelo}   GET          Última recomendación del advertiser para el modelo (TopCTR o TopProduct).
  /history/{advertiser\_id}/                   GET          Recomendaciones disponibles, filtradas por últimos 7 días. Ver nota.
  /stats/                                      GET          Estadísticas agregadas, incluyendo similaridad Jaccard entre modelos.
  -------------------------------------------- ------------ ---------------------------------------------------------------------------

Sobre /history/ hay que aclarar algo. La query SQL filtra por date \>= (hoy - 7 días), o sea, pide siete días de historia. Pero como las tablas se reescriben en cada run del DAG, en la práctica solo hay un día en la base. Por eso este endpoint devuelve un único día. Lo dejamos documentado como mejora pendiente en la sección 9.

Los códigos HTTP que devuelve la API son los estándar de REST: 200 para todo bien, 400 si te mandaste alguna macana en el request (por ejemplo pedir un modelo que no existe), 404 si no hay datos para ese advertiser, y 500 si algo se rompe en el servidor (en cuyo caso la traza queda en Cloud Logging y la podemos debugear leyendo logs).

5\. Los dos modelos de recomendación

5.1 TopCTR: ordenar por click-through ratio

El modelo TopCTR ordena los productos del advertiser por su CTR (click-through ratio), que es la fracción de impresiones que terminaron en click. La fórmula es directa:

CTR(advertiser, producto) = clicks / impresiones

Para evitar que el código explote por dividir por cero, usamos un guardia: if r\[\"impressions\_count\"\] \> 0 else 0. Productos sin impresiones obtienen CTR=0 automáticamente.

Importante decir esto, porque es una limitación conocida: el código actual NO aplica un umbral mínimo de impresiones para descartar productos con muestras chicas. ¿Qué significa? Que un producto con 2 impresiones y 2 clicks tiene CTR=1.0 (que es el máximo posible) y puede aparecer primero en el ranking, aunque la muestra sea estadísticamente débil. Para el dataset usado, los volúmenes son lo suficientemente altos como para que este efecto sea menor en la práctica, pero es una limitación reconocida y la dejamos documentada como primera mejora en la sección 9.

Para cada advertiser nos quedamos con los veinte productos con mejor CTR.

Este modelo prioriza productos que generan engagement: aunque tengan menos visualizaciones absolutas, son los que mejor convierten cuando se muestran. Es el modelo a elegir si el anunciante optimiza por conversión, no por alcance.

5.2 TopProduct: ordenar por cantidad de visualizaciones

El modelo TopProduct va por el otro lado: ordena los productos por cantidad absoluta de visualizaciones (las que figuran en product\_views), sin importar si esas visualizaciones generaron clicks o no. Para cada advertiser nos quedamos con los veinte productos más vistos del día.

Este modelo captura una señal totalmente distinta a TopCTR. Un producto puede tener muchísimas vistas y bajo CTR (productos populares pero no muy bien orientados a la audiencia), o al revés. Por eso ambos modelos son complementarios y no redundantes: cada uno responde una pregunta diferente del negocio.

5.3 Comparando los dos modelos: similaridad de Jaccard

Tener dos modelos que recomiendan productos para el mismo advertiser nos dejó una pregunta natural: ¿qué tan parecidas son sus recomendaciones? Si fueran muy parecidas, mantener los dos sería redundante; si fueran muy distintas, los dos aportan información complementaria. Para responder esto usamos una métrica clásica de comparación entre conjuntos: el coeficiente de similaridad de Jaccard.

La idea es simple: medir cuánto se solapan dos conjuntos. Si tenemos un conjunto A y un conjunto B, Jaccard mira cuántos elementos están en los dos (intersección) y los divide por cuántos elementos están en cualquiera (unión):

Jaccard(A, B) = \|A interseccion B\| / \|A union B\|

El resultado es un número entre 0 y 1, donde 0 significa \"los conjuntos no comparten nada\" y 1 significa \"son exactamente iguales\". Para entenderlo con un ejemplo cotidiano, supongamos dos canastas de frutas:

Canasta A: { manzana, pera, banana, kiwi, uva }

Canasta B: { banana, kiwi, naranja, frutilla, melon }

Frutas en las dos canastas: { banana, kiwi } -\> 2 elementos

Frutas en cualquiera de las dos: 8 elementos

Jaccard = 2 / 8 = 0.25

Las dos canastas comparten el 25% de las frutas si las consideramos juntas.

En nuestro caso, A son los 20 productos recomendados por TopCTR para un advertiser, y B los 20 recomendados por TopProduct para el mismo advertiser. El cálculo lo hacemos en el endpoint /stats/ con código Python bien compacto:

ctr\_set = set(productos\_topctr)

prod\_set = set(productos\_topproduct)

overlap = len(ctr\_set & prod\_set) \# interseccion (operador & en sets)

union = len(ctr\_set \| prod\_set) \# union (operador \| en sets)

jaccard = overlap / union if union else 0

Cómo interpretar los valores:

0.00 - 0.10 Modelos casi disjuntos (recomiendan productos muy distintos)

0.10 - 0.30 Solapamiento bajo

0.30 - 0.60 Solapamiento medio

0.60 - 0.90 Solapamiento alto

0.90 - 1.00 Casi identicos. Uno seria redundante

En la corrida de evaluación que hicimos, los valores de Jaccard que arrojó la API se ubicaron entre 0.053 y 0.212 entre los veinte advertisers procesados. Esto significa que los dos modelos comparten en promedio entre el 5% y el 21% de los productos recomendados. La conclusión práctica es que tener ambos modelos en el sistema aporta valor real, no son redundantes. Si fueran idénticos (Jaccard cerca de 1) podríamos descartar uno.

Visto desde el negocio: TopCTR encuentra productos que convierten bien cuando se muestran, mientras que TopProduct encuentra productos que tienen mucha exposición. Son objetivos complementarios y los anunciantes pueden usar uno u otro según optimicen por conversión o por alcance.

6\. Cómo lo desplegamos

6.1 Empaquetar la API con Docker

La API se empaqueta en un container Docker para que corra igual en cualquier lado: en nuestra máquina, en la del corrector, en Cloud Run. La imagen se construye con un Dockerfile cortito (8 líneas) basado en python:3.12-slim.

Una decisión chiquita pero importante es el orden de las instrucciones: copiamos primero el archivo requirements.txt y hacemos el pip install (que tarda 30-60 segundos), y recién después copiamos el código. ¿Por qué? Por el caché de capas de Docker. Si solo cambia el código, Docker reutiliza el caché del pip install y no reinstala las dependencias. Si lo hiciéramos al revés, cada cambio en el código nos forzaría a reinstalar todo. Es una buena práctica que aprendimos durante el TP.

El container expone el puerto 8080 y arranca uvicorn con el bind 0.0.0.0. Esto último es importante: 0.0.0.0 significa \"escuchá en todas las interfaces de red\", lo cual es necesario para que Cloud Run pueda recibir requests externos. Si dejáramos 127.0.0.1 (localhost), el container solo se hablaría a sí mismo y nadie de afuera podría llegarle.

6.2 El pipeline de deploy: build, push, deploy

El despliegue tiene tres pasos manuales pero versionados:

1\. Build: gcloud builds submit api/ \--tag=\...:vN

2\. Push: (lo hace Cloud Build solo al final del build)

3\. Deploy: gcloud run deploy fastapi-tp \--image=\...:vN

Cloud Build agarra la carpeta api/, la sube a un bucket temporal, levanta una VM efímera con Docker (que después se destruye), ejecuta cada paso del Dockerfile, y sube la imagen final a Artifact Registry. Tarda alrededor de un minuto. Una vez que la imagen está en el registry, gcloud run deploy le dice a Cloud Run que use esa nueva imagen, crea una revisión nueva e inmutable y le manda el tráfico al container nuevo. Lo lindo es que no hay downtime: Cloud Run no apaga el container viejo hasta que el nuevo está respondiendo OK.

6.3 Versionado de imágenes y rollback

Cada deploy lo hicimos con un tag explícito (v1, v2, v3, v4) en lugar del tag :latest, que sería el default. Esto agrega trabajo manual mínimo (acordarse de incrementar el número), pero a cambio nos da algo importante: si una versión nueva trae bugs, podemos volver a la anterior con un solo deploy, porque la imagen anterior sigue viva en Artifact Registry. A esto se le llama rollback.

Cloud Run también guarda un historial de revisiones del servicio. Eso permite, si quisiéramos, dividir el tráfico entre revisiones (lo que se llama canary deployment: \"mandá el 90% del tráfico a la versión vieja y el 10% a la nueva, así si algo se rompe afecta a poca gente\"). En este TP no lo usamos, pero es bueno saberlo.

6.4 Variables de entorno y manejo de secretos

La conexión a Cloud SQL la configuramos por una variable de entorno llamada POSTGRES\_URI, definida directamente en el servicio de Cloud Run. ¿Qué es una variable de entorno? Es un valor que se le pasa a un proceso por afuera del código, como una caja con configuración. Sirve para que la app no tenga que tener cosas como passwords escritas en el código fuente.

Esto sigue el principio de Twelve-Factor App, que es un set de buenas prácticas para apps modernas: la configuración va por env vars, no commiteada al código. En el repositorio tenemos un archivo .env.example que muestra el formato esperado, con un placeholder TU\_PASSWORD en lugar de la password real. Así cualquiera que clone el repo entiende qué variables necesita sin ver credenciales.

La API tiene una mini capa que normaliza la URI antes de usarla. Si llega con un prefijo extendido del estilo postgresql+psycopg2://, la traduce al formato libpq estándar (postgresql://) que es el que entiende psycopg2 directamente. Esto lo agregamos cuando descubrimos que la URI ya seteada en Cloud Run venía con ese prefijo y la API explotaba al arrancar. Lo contamos con detalle en la sección 8.

6.5 Networking: la IP estática y el firewall

La VM de Airflow tiene una IP externa estática (34.55.205.251), reservada como recurso aparte en GCP. La promovimos de IP efímera (que cambia si la VM se reinicia) a estática (que queda fija para siempre) con un solo comando. Sin esto, cualquier reinicio podría cambiarle la IP y la URL que le pasamos al corrector quedaría rota.

La regla de firewall allow-airflow-ui permite tráfico entrante TCP en el puerto 8080 desde cualquier origen (0.0.0.0/0). Suena medio permisivo, pero es aceptable porque Airflow tiene su propia autenticación con usuario y password: aunque cualquiera llegue al puerto, sin login no entra.

La API en Cloud Run, por su parte, se expone a través de una URL HTTPS estable que provee la plataforma. No tuvimos que gestionar certificados SSL, IPs externas ni balanceadores de carga: todo eso lo hace Cloud Run debajo. Una de las ventajas de un serverless administrado.

7\. Las decisiones que tomamos en el camino

Acá contamos las decisiones que fueron cambiando durante el desarrollo. No siempre acertamos a la primera, y eso es parte del aprendizaje.

7.1 Endpoints con path params en lugar de query params

La primera versión de la API tenía endpoints con la forma /recommendations/{advertiser\_id}?model=TopCTR. Es decir, el modelo iba como query parameter (los que van después del signo de pregunta). Cuando releímos la consigna del TP nos dimos cuenta que pedía el modelo como path parameter (parte de la URL): /recommendations/{advertiser\_id}/{modelo}.

La diferencia es chiquita pero importante: con query params, el modelo es opcional y se manda como ?model=X. Con path params, es parte de la URL y es obligatorio. La consigna pedía path params, así que reescribimos los endpoints. Esto nos forzó también a agregar validación: si alguien manda un modelo que no existe, devolvemos 400 Bad Request con un mensaje claro en lugar de explotar.

7.2 psycopg2 directo en la API, SQLAlchemy via pandas en el DAG

La API usa psycopg2 directo. El DAG usa pandas.to\_sql, que internamente requiere un engine de SQLAlchemy. Es una asimetría: dos componentes del mismo sistema usan dos abstracciones distintas para hablarle a la misma base.

La justificación es histórica: el DAG arrancó con la receta clásica de pandas-a-Postgres (to\_sql), que es lo que se enseña en cualquier tutorial de Data Engineering. La API arrancó separadamente con psycopg2.connect porque las queries son simples y no necesitábamos un ORM. Cuando lo notamos, decidimos no unificar para no introducir cambios de último momento.

7.3 Cloud Run en lugar de una VM dedicada para la API

Para la API también podríamos haber usado una VM, pero elegimos Cloud Run por varias razones. Tiene autoscaling automático (si llegan muchas peticiones, levanta más containers; si no llegan, escala a cero y no se paga nada). Maneja TLS automáticamente (no nos peleamos con certificados SSL). El deploy es trivial. La contra es la latencia de cold start: el primer request después de un rato sin tráfico puede tardar uno o dos segundos extra mientras Cloud Run levanta el container. Para los volúmenes que esperamos en este TP, no es problema.

7.4 Versionar imágenes Docker con vN en vez de :latest

Las imágenes Docker las subimos a Artifact Registry con tags explícitos (v1, v2, v3, v4) en lugar de pisar siempre :latest, que sería el default. Esto agrega trabajo manual mínimo, pero a cambio nos da rollback inmediato si una versión nueva trae bugs: la imagen anterior sigue ahí, basta con redeployarla. Si pisáramos :latest todo el tiempo, perderíamos esa red de seguridad.

8\. Lo que se nos rompió y cómo salimos

8.1 La connection string que la API no entendía

Síntoma: después de un deploy, los endpoints empezaron a tirar 500 Internal Server Error. /health respondía OK, pero cualquier endpoint que tocara la base explotaba. Encontrar la causa nos llevó un rato hasta que fuimos a leer los logs en Cloud Run.

El mensaje exacto era:

psycopg2.ProgrammingError: invalid dsn: missing \"=\" after

\"postgresql+psycopg2://\...\" in connection info string

Resulta que la URI seteada en la variable de entorno tenía un prefijo extendido (postgresql+psycopg2://) que algunas librerías como SQLAlchemy aceptan, pero psycopg2 directo no. Cuando psycopg2 ve un string que no arranca con postgresql:// limpio, intenta interpretarlo como un DSN de tipo \"clave=valor\" separado por espacios, y al no encontrar el = se rompe.

La solución fue agregar tres líneas al inicio de main.py que normalizan la URI: si arranca con postgresql+psycopg2://, lo reemplazamos por postgresql:// limpio. Así el código se vuelve robusto frente a las dos formas de escribir la URI, y no tuvimos que tocar la variable de entorno en Cloud Run, lo cual evita la fricción de redeployar configuración.

8.2 El scheduler de Airflow huérfano sin variables de entorno

Síntoma: durante varios días el DAG no procesó datos nuevos. La API seguía respondiendo (porque las tablas de la base seguían teniendo los datos viejos) pero la fecha más reciente que devolvía empezó a ser cada vez más vieja. Cuando lo notamos, fuimos a investigar.

Lo primero que probamos fue listar los DAGs registrados en Airflow:

\$ airflow dags list

Error: Failed to load all files. For details, run

\`airflow dags list-import-errors\`

No data found

Corriendo list-import-errors apareció el problema:

RuntimeError: Falta la variable de entorno POSTGRES\_URI.

Definirla en \~/.bashrc del usuario que corre Airflow.

La variable estaba en .bashrc, sí. El tema es que .bashrc solo lo lee una shell interactiva (cuando hacés login y aparece el prompt). El proceso del scheduler había arrancado en algún momento desde una shell que tenía la variable, pero después esa shell se cerró y el scheduler quedó huérfano. Un proceso huérfano en Linux es uno cuyo padre murió: el sistema operativo lo re-asigna al proceso init (PID 1), por eso se reconoce viendo PPID=1. Los workers que Airflow lanza desde ese scheduler huérfano no heredan más la variable de la shell original que ya no existe.

La solución fue matar todos los procesos viejos, abrir una shell limpia que cargara .bashrc (haciendo source \~/.bashrc), y volver a arrancar el scheduler y el webserver con nohup. Para una solución más definitiva habría que crear unidades systemd con la variable definida en el archivo .service, pero por tiempo lo dejamos para una mejora futura.

8.3 La memoria del proyecto que tenía un nombre de DAG equivocado

Mientras debugueábamos el problema anterior, intentamos verificar los runs históricos del DAG con:

airflow dags list-runs -d adtech\_recos

Y nos devolvía vacío. Pensábamos que el DAG estaba sin runs, pero el problema era otro: en el archivo del DAG el dag\_id estaba puesto como adtech\_pipeline, no adtech\_recos. Habíamos cambiado el nombre en algún punto del desarrollo y no lo registramos.

Es un problema chiquito pero ilustra algo importante: los nombres son contratos. Si el dag\_id en el código no matchea con lo que esperan los comandos o la documentación, todo se rompe. La lección fue revisar siempre el código fuente como fuente de verdad, no las notas viejas.

9\. Lo que quedó para mejorar

El sistema cumple con los objetivos pero hay varias áreas donde nos hubiera gustado avanzar más. Las dejamos documentadas porque creemos que vale la pena ser honestos sobre el estado real:

-   Tablas reescritas en cada run del DAG. El db\_writing usa pandas.to\_sql con if\_exists=\"replace\", por lo que las tablas se borran y recrean enteras en cada corrida. Como consecuencia, /history/{advertiser\_id}/ devuelve un solo día en lugar de los siete que sugiere la firma del endpoint. La mejora directa sería cambiar a un esquema con DELETE WHERE date=ds + INSERT, manteniendo histórico real.

-   TopCTR sin filtro de impresiones mínimas. El modelo no descarta productos con pocas impresiones. Un producto con 2 impresiones y 2 clicks tiene CTR=1.0 y puede ganar el ranking. La mejora directa sería agregar un filtro tipo impressions\_count \>= N (por ejemplo N=10) antes de calcular el ranking.

-   DAG con SQLAlchemy via pandas, API con psycopg2 directo. Dos formas de hablarle a la misma base. Una mejora prolija sería unificar ambos componentes en psycopg2 directo.

-   El DAG no descarga directamente de GCS. Lee CSVs locales de /home/pipeposse/trabajo\_practico/. Hay que bajar los archivos manualmente con gsutil cp. Una mejora sería usar la librería google-cloud-storage desde el DAG para que cada run descargue lo que necesita.

-   Tests automatizados. La API y el DAG no tienen tests unitarios ni de integración. En un sistema de producción real escribiríamos tests con pytest para cada endpoint y para el flujo de transformación. Los validamos manualmente con curl, pero no es lo mismo.

-   Lock file de dependencias. El requirements.txt tiene versiones pineadas con ==, pero no incluye hashes ni las dependencias transitivas explícitas. Una mejora sería usar pip-compile o Poetry para generar un lock file completo y reproducible.

-   Conexión a Cloud SQL más segura. La API se conecta a la base por IP pública. Cloud Run permite conectarse via Cloud SQL Auth Proxy, lo que evita exponer la base por internet.

-   Monitoreo y alertas. No tenemos alertas configuradas para fallos del DAG ni del servicio. Cloud Monitoring permitiría detectar regresiones automáticamente sin que tengamos que mirar logs.

-   Autenticación de la API. Los endpoints son públicos. Para un escenario real habría que poner API keys, OAuth o IAM.

-   Servicio systemd para Airflow. El scheduler y webserver corren con nohup. Lo más prolijo serían archivos .service de systemd para que se reinicien al boot de la VM y sobrevivan a reinicios automáticos.

10\. Conclusiones

Este TP nos llevó por todo el ciclo de un sistema de datos en producción: desde archivos crudos en un bucket hasta una API HTTPS pública pasando por orquestación, base de datos relacional, containers, deploy versionado, y debugging de errores reales en producción. Cada pieza era un mundo aparte, y juntarlas en un sistema que corre de punta a punta fue donde más aprendimos.

Los servicios administrados de GCP que usamos (Cloud Storage, Compute Engine, Cloud SQL, Artifact Registry, Cloud Build, Cloud Run) cubrieron los componentes de infraestructura sin que tuviéramos que pelearnos con servidores propios. Eso fue clave: pudimos enfocar el tiempo en la lógica del pipeline y en el código, no en levantar máquinas a mano.

Los dos modelos que implementamos, TopCTR y TopProduct, nos sorprendieron al medir su similaridad con Jaccard. Esperábamos quizás que fueran más parecidos, pero los valores entre 0.053 y 0.212 nos mostraron que están capturando señales distintas. Eso valida la decisión de mantener ambos en el sistema: si fueran muy parecidos, uno sería redundante.

Pero más allá de lo funcional, lo más valioso del trabajo fue lo que aprendimos del lado del oficio: cómo se lee un stack trace en logs centralizados, por qué un proceso huérfano pierde sus variables de entorno, cómo Cloud Build genera capas Docker reutilizables, qué significa realmente que un servicio sea \"stateless\". Son cosas que en clase se mencionan rápido y acá las vivimos en carne propia debugueando errores reales a las once de la noche.

Nos vamos del TP con una caja de herramientas más grande y, sobre todo, con un proyecto que podemos abrir y entender de punta a punta, incluyendo sus limitaciones. Eso era el objetivo desde el principio.

11\. Anexos

A. URLs públicas y credenciales para evaluación

  ------------------------------------- -------------------------------------------------
  **Recurso**                           **Acceso**
  API REST (Cloud Run)                  https://fastapi-tp-xbo6kajhza-uc.a.run.app
  Documentación interactiva (Swagger)   https://fastapi-tp-xbo6kajhza-uc.a.run.app/docs
  Airflow UI                            http://34.55.205.251:8080
  Repositorio Git                       https://github.com/pipeposse/tp-final-adtech
  ------------------------------------- -------------------------------------------------

Credenciales de Airflow para evaluación (rol Viewer, solo lectura):

Usuario: profesor

Password: profesor

B. Comandos de despliegue

Construcción de imagen Docker via Cloud Build:

gcloud builds submit api/ \\

\--tag=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:vN \\

\--project=tp-final-adtech-493922

Despliegue a Cloud Run:

gcloud run deploy fastapi-tp \\

\--image=us-central1-docker.pkg.dev/tp-final-adtech-493922/tp-final-repo/fastapi-tp:vN \\

\--region=us-central1 \--project=tp-final-adtech-493922

Reserva de IP estática para la VM de Airflow:

gcloud compute addresses create diegue-pipe-belu-airflow-ip \\

\--addresses=34.55.205.251 \\

\--region=us-central1 \--project=tp-final-adtech-493922

Creación del usuario profesor en Airflow:

/home/pipeposse/airflow\_venv/bin/airflow users create \\

\--username profesor \--firstname Profesor \--lastname TPFinal \\

\--role Viewer \--email profesor\@tp-adtech.local \--password \'profesor\'

C. Ejemplos de respuestas de la API

Respuesta del endpoint /recommendations/{advertiser\_id}/TopCTR:

{

\"advertiser\_id\": \"LW045DVYSGRD75TK6U54\",

\"model\": \"TopCTR\",

\"date\": \"2026-05-01\",

\"recommendations\": \[\"0639y2\", \"1mlwn4\", \"2bx2zk\", \...\]

}

Fragmento de respuesta de /stats/, sección de coincidencia entre modelos:

\"coincidencia\_entre\_modelos\": \[

{ \"advertiser\_id\": \"LW045DVYSGRD75TK6U54\",

\"productos\_en\_comun\": 7, \"jaccard\": 0.212 },

\...

\]

D. Ejemplos de consulta a la API

Para que el evaluador pueda probar la API directamente desde un navegador, dejamos URLs listas para pegar. Como advertiser de prueba usamos LW045DVYSGRD75TK6U54, que es uno real del dataset y es ademas el que tiene mayor similaridad de Jaccard entre los dos modelos (0.212), por lo que sirve para ver el sistema en plenitud.

**Health check (verifica que la API este viva)**

https://fastapi-tp-xbo6kajhza-uc.a.run.app/health

Respuesta esperada: {\"status\":\"ok\"}. No toca la base, solo confirma que el container esta respondiendo.

**Ver recomendaciones por TopCTR para un advertiser**

https://fastapi-tp-xbo6kajhza-uc.a.run.app/recommendations/LW045DVYSGRD75TK6U54/TopCTR

Devuelve un JSON con el advertiser\_id, el modelo (TopCTR), la fecha de la ultima corrida del DAG, y la lista de los 20 productos recomendados ordenados por click-through ratio.

**Ver recomendaciones por TopProduct para el mismo advertiser**

https://fastapi-tp-xbo6kajhza-uc.a.run.app/recommendations/LW045DVYSGRD75TK6U54/TopProduct

Mismo formato pero con los 20 productos ordenados por cantidad de visualizaciones. Los productos van a ser distintos a los de TopCTR (es la confirmacion practica de que los modelos no son redundantes).

**Ver el historial de un advertiser**

https://fastapi-tp-xbo6kajhza-uc.a.run.app/history/LW045DVYSGRD75TK6U54/

Devuelve los ultimos 7 dias de recomendaciones para ambos modelos. Como aclaramos en la seccion 9, en la implementacion actual del DAG las tablas se reescriben en cada run, por lo que este endpoint devuelve un solo dia.

**Ver estadisticas agregadas y analisis Jaccard**

https://fastapi-tp-xbo6kajhza-uc.a.run.app/stats/

Devuelve un JSON con cuatro secciones: cantidad de consultas por dia, cantidad de advertisers procesados por modelo, y una lista ordenada por similaridad Jaccard donde cada fila muestra cuantos productos comparten los modelos TopCTR y TopProduct para cada advertiser.

**Probar un caso de error - modelo invalido**

https://fastapi-tp-xbo6kajhza-uc.a.run.app/recommendations/LW045DVYSGRD75TK6U54/ModeloInventado

Respuesta esperada: HTTP 400 con el mensaje \"Modelo invalido: \'ModeloInventado\'. Opciones: \[\'TopCTR\', \'TopProduct\'\]\". Es la confirmacion de que la API valida los inputs en lugar de explotar.

**Probar un caso de error - advertiser inexistente**

https://fastapi-tp-xbo6kajhza-uc.a.run.app/recommendations/ADV\_INEXISTENTE/TopCTR

Respuesta esperada: HTTP 404 con el mensaje \"No hay recomendaciones para advertiser \'ADV\_INEXISTENTE\' con modelo \'TopCTR\'\".

**Otros advertiser\_id reales para probar**

Cualquiera de los siguientes funciona y devuelve resultados:

LW045DVYSGRD75TK6U54 (Jaccard 0.212, el mas alto)

OY5LNPB5A8FF43ITRZG3 (Jaccard 0.212)

2WPF1NXECF3G6NUMWDO7 (Jaccard 0.176)

AK81O7W3KGPEN8LABG2N (Jaccard 0.176)

K6Z0X85ZUY0TSF4RCG5J (Jaccard 0.176)

IDOFCO721HTJGDH7332G (Jaccard 0.053, el mas bajo de la lista)

**Forma alternativa: documentacion interactiva**

En lugar de armar las URLs a mano, recomendamos abrir https://fastapi-tp-xbo6kajhza-uc.a.run.app/docs y usar la interfaz Swagger. Cada endpoint tiene un boton \"Try it out\" que permite escribir los parametros en formularios y ejecutar la consulta sin tener que pensar la URL.
