CREATE TABLE IF NOT EXISTS clientes (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS ordenes (
    id SERIAL PRIMARY KEY,
    descripcion VARCHAR(255) NOT NULL,
    cliente_id INTEGER NOT NULL,
    FOREIGN KEY (cliente_id) REFERENCES clientes(id)
);

CREATE TABLE IF NOT EXISTS productos (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    stock INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);

-- nuevas columnas del caso de uso
ALTER TABLE productos ADD COLUMN IF NOT EXISTS category VARCHAR(64);
ALTER TABLE productos ADD COLUMN IF NOT EXISTS reserved INTEGER NOT NULL DEFAULT 0;
ALTER TABLE productos DROP CONSTRAINT IF EXISTS productos_stock_no_negativo;
ALTER TABLE productos ADD CONSTRAINT productos_stock_no_negativo CHECK (stock >= 0) NOT VALID;

CREATE TABLE IF NOT EXISTS orden_compra (
    id BIGSERIAL PRIMARY KEY,
    cliente_id BIGINT,
    region VARCHAR(16),
    idempotency_key VARCHAR(128),
    estado VARCHAR(20) NOT NULL,
    subtotal DECIMAL(12, 2),
    impuesto DECIMAL(12, 2),
    total DECIMAL(12, 2),
    risk_score INTEGER,
    creado_en TIMESTAMPTZ NOT NULL DEFAULT now(),
    expira_en TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_orden_compra_idem ON orden_compra (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_orden_compra_estado_expira ON orden_compra (estado, expira_en);

CREATE TABLE IF NOT EXISTS orden_item (
    id BIGSERIAL PRIMARY KEY,
    orden_id BIGINT NOT NULL REFERENCES orden_compra(id) ON DELETE CASCADE,
    producto_id BIGINT NOT NULL,
    categoria VARCHAR(64),
    cantidad INTEGER NOT NULL,
    precio_unitario DECIMAL(12, 2)
);
CREATE INDEX IF NOT EXISTS ix_orden_item_orden ON orden_item (orden_id);

CREATE TABLE IF NOT EXISTS evento_inventario (
    id BIGSERIAL PRIMARY KEY,
    tipo VARCHAR(20) NOT NULL,
    producto_id BIGINT NOT NULL,
    delta INTEGER NOT NULL,
    orden_id BIGINT,
    ocurrido_en TIMESTAMPTZ NOT NULL DEFAULT now()
);
