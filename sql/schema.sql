-- sql/schema.sql
--
-- Esquema de la base `recos_db` (PostgreSQL 18 en Cloud SQL).
-- Refleja el estado real de la base al 2026-05-09 despues de la migracion
-- al modelo normalizado.
--
-- IMPORTANTE: este archivo es la fuente de verdad del schema. Las tablas
-- y los indices se crean idealmente desde este script. Algunos objetos
-- los crea la API o el DAG en runtime (ver notas):
--   - `recommendations`: la crea este script. El DAG la llena con upsert
--      (`INSERT ... ON CONFLICT ... DO UPDATE`).
--   - `api_logs`: la crea la API al arrancar (handler de lifespan).
-- =========================================================================
-- recommendations - resultado de los modelos (normalizada: una fila por
--                   producto recomendado).
-- =========================================================================
CREATE TABLE recommendations (
    id            BIGSERIAL PRIMARY KEY,
    advertiser_id VARCHAR(64)  NOT NULL,
    model         VARCHAR(32)  NOT NULL,   -- valores: 'top_ctr', 'top_product'
    product_id    VARCHAR(128) NOT NULL,
    rank          INTEGER      NOT NULL,
    score         NUMERIC(10,6),
    date          DATE         NOT NULL,
    created_at    TIMESTAMP DEFAULT NOW()
);

-- Indice unico necesario para el ON CONFLICT del DAG (DBWriting).
-- El orden de columnas no importa para ON CONFLICT, pero matchea con la
-- tupla (advertiser, model, producto, dia) que define la clave natural.
CREATE UNIQUE INDEX uq_recommendations_natural_key
    ON recommendations (advertiser_id, model, product_id, date);

-- Indice de lookup por modelo: usado por la API para devolver
-- `/recommendations/{advertiser}/{modelo}` con LIMIT 20 ordenado por
-- date DESC, rank.
CREATE INDEX idx_reco_lookup
    ON recommendations (advertiser_id, model, date DESC);

-- Indice de history por advertiser: usado por `/history/{advertiser}/`
-- que filtra por advertiser y rango de fechas.
CREATE INDEX idx_reco_history
    ON recommendations (advertiser_id, date DESC);

-- =========================================================================
-- api_logs - registro de cada consulta exitosa al endpoint /recommendations
--            (la crea FastAPI en lifespan startup, queda aqui documentada).
-- =========================================================================
CREATE TABLE IF NOT EXISTS api_logs (
    id            SERIAL PRIMARY KEY,
    advertiser_id TEXT,
    model         TEXT,         -- valor PascalCase: 'TopCTR' / 'TopProduct'
    date          TEXT,         -- fecha de la consulta, ISO YYYY-MM-DD
    timestamp     TIMESTAMP DEFAULT NOW()
);

-- =========================================================================
-- Historial de cambios del schema:
--   2026-05-09: migracion a modelo normalizado.
--     - DROP de tablas viejas top_ctr y top_product (formato lista TEXT).
--     - CREATE de tabla recommendations + indice unico
--       uq_recommendations_natural_key.
--     - La API se reescribio (v3) para leer de recommendations en vez de
--       las tablas viejas.
-- =========================================================================
