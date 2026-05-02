-- sql/schema.sql
--
-- Esquema descriptivo de la base recos_db (PostgreSQL 15 en Cloud SQL).
--
-- IMPORTANTE: este archivo es solo documentacion. Las tablas NO se crean
-- desde este script en runtime.
--   - top_ctr y top_product las crea el DAG en la tarea DBWriting usando
--     pandas.to_sql(..., if_exists="replace"), por lo que se reescriben
--     enteras en cada corrida del DAG.
--   - api_logs la crea la API al arrancar (en el handler de lifespan).
--
-- Esta SQL refleja la estructura efectiva resultante.


-- =========================================================================
-- top_ctr - resultado del modelo TopCTR del ultimo run del DAG
-- =========================================================================
CREATE TABLE top_ctr (
    advertiser_id TEXT,
    top_products  TEXT,  -- lista de Python serializada: "['p1', 'p2', ...]"
    date          TEXT
);


-- =========================================================================
-- top_product - resultado del modelo TopProduct del ultimo run del DAG
-- =========================================================================
CREATE TABLE top_product (
    advertiser_id TEXT,
    top_products  TEXT,  -- lista de Python serializada: "['p1', 'p2', ...]"
    date          TEXT
);


-- =========================================================================
-- api_logs - registro de cada consulta exitosa al endpoint /recommendations
-- =========================================================================
CREATE TABLE api_logs (
    id            SERIAL PRIMARY KEY,
    advertiser_id TEXT,
    model         TEXT,
    date          TEXT,
    timestamp     TIMESTAMP DEFAULT NOW()
);
