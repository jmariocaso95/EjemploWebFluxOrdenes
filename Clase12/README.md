# Demo · Checkout reactivo sobre R2DBC

Spring Boot 4.1.1 · Java 17 · WebFlux · Spring Data R2DBC + PostgreSQL 15 · Project Reactor.
App en `localhost:8081`. Caso de uso completo en `../CASO_DE_USO_REACTIVO.md`.

## Base de datos con podman

No hace falta Docker Desktop. Dos caminos, ambos probados:

```bash
./scripts/db.sh up        # podman directo: arranca la máquina si hace falta y espera a pg_isready
podman-compose up -d      # equivalente con docker-compose.yml
```

Comandos del script:

| Comando | Qué hace |
|---|---|
| `./scripts/db.sh up` | Crea/arranca `postgres_r2dbc` en 5432 y espera a que acepte conexiones |
| `./scripts/db.sh down` | Detiene el contenedor (conserva los datos del volumen `pgdata`) |
| `./scripts/db.sh reset` | Borra contenedor **y volumen**, vuelve a arrancar en limpio |
| `./scripts/db.sh psql` | Abre `psql` dentro del contenedor (acepta `-c 'SELECT ...'`) |
| `./scripts/db.sh logs` | Sigue los logs de Postgres |
| `./scripts/db.sh status` | Estado del contenedor |

## Arrancar y probar

```bash
./scripts/db.sh up
./gradlew test            # 27 tests, incluye el flujo E2E contra Postgres
./gradlew bootRun         # app en 8081
```

Sembrar catálogo y crear una orden:

```bash
curl -X POST localhost:8081/api/products/bulk -H 'Content-Type: application/x-ndjson' \
  --data-binary $'{"id":1,"name":"Teclado","price":100,"stock":10,"category":"PERIFERICOS"}\n{"id":2,"name":"Mouse","price":50,"stock":3,"category":"PERIFERICOS"}\n{"id":3,"name":"Monitor","price":900,"stock":0,"category":"PANTALLAS"}\n'

curl -X POST localhost:8081/api/clientes -H 'Content-Type: application/json' \
  -d '{"nombre":"Ana","email":"ana@mail.com"}'

curl -i -X POST localhost:8081/api/orders -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: demo-1' -H 'Idempotency-Key: K1' \
  -d '{"clienteId":1,"region":"CO","items":[{"productoId":1,"cantidad":2},{"productoId":2,"cantidad":1}]}'
```

Inspeccionar la base (equivalente podman del `docker exec` del documento):

```bash
./scripts/db.sh psql -c 'SELECT id, estado, subtotal, impuesto, total, risk_score FROM orden_compra ORDER BY id DESC LIMIT 5;' \
                    -c 'SELECT id, name, stock, reserved FROM productos ORDER BY id;' \
                    -c 'SELECT tipo, producto_id, delta, orden_id FROM evento_inventario ORDER BY id DESC LIMIT 10;'
```

## OpenAPI / Postman

El contrato se genera del código con springdoc. Con la app arriba:

| URL | Qué devuelve |
|---|---|
| `http://localhost:8081/swagger-ui.html` | Swagger UI para probar a mano |
| `http://localhost:8081/v3/api-docs` | OpenAPI 3.0.1 en JSON |
| `http://localhost:8081/v3/api-docs.yaml` | El mismo contrato en YAML |

Exportarlo a disco sin arrancar nada a mano (`docs/openapi.json` y `docs/openapi.yaml`):

```bash
./scripts/db.sh up
./gradlew exportOpenApi
```

La tarea levanta el `bootJar` en el puerto 18099, descarga el contrato y apaga el proceso.

**Importar en Postman:** `Import` → `Files` → `docs/openapi.json` (o pegar la URL
`http://localhost:8081/v3/api-docs` en `Import → Link`). Postman crea una colección con las 23 operaciones
agrupadas por tag y los headers `Idempotency-Key` y `X-Correlation-Id` ya declarados. En `Variables` de la
colección, `baseUrl` queda en `http://localhost:8081`.

Detalles útiles al probar desde Postman:

- `POST /api/products/bulk` es NDJSON: cuerpo `raw`, tipo `application/x-ndjson`, **un JSON por línea**
  (Postman lo importa como array de ejemplo; hay que pegar las líneas sueltas).
- `GET /api/orders/{id}/events`, `GET /api/ops/dashboard` y `GET /api/reports/sales/stream` son respuestas
  en streaming. Postman las muestra cuando cierran; `/dashboard` y `/sales/stream` no cierran nunca,
  así que para verlas en vivo conviene `curl -N`.
- El contrato sale en **OpenAPI 3.0.1** por compatibilidad. Para emitir 3.1, cambiar
  `springdoc.api-docs.version` a `openapi_3_1` en `application.yml`.

## Endpoints

| Método | Ruta | Descripción |
|---|---|---|
| POST | `/api/orders` | Crea orden. Headers opcionales `Idempotency-Key`, `X-Correlation-Id` |
| GET | `/api/orders/{id}` | Orden con sus ítems |
| POST | `/api/orders/{id}/confirm` | Confirma (reserva → venta) |
| GET | `/api/orders/{id}/events` | SSE de una orden, cierra en estado terminal |
| GET | `/api/ops/dashboard` | SSE global compartido (hot) |
| GET | `/api/reports/sales` | Totales por categoría (JSON) |
| GET | `/api/reports/sales/stream` | Acumulado en vivo (NDJSON) |
| POST | `/api/products/bulk` | Carga NDJSON en lotes de 500 (upsert) |
| GET/PUT/DELETE | `/external/simulator` | Fallos/latencia/riesgo de los servicios simulados |

Errores: 404 `ProductoNoExisteException` · 409 `StockInsuficienteException` · 422 `RiesgoAltoException` ·
404 `OrdenNoExisteException` · 409 `EstadoInvalidoException` · 400 `ValidacionException`.

## Notas de entorno

- `podman compose` (subcomando) delega en `/usr/local/bin/docker-compose` y falla con
  `docker-credential-desktop: executable file not found` porque `~/.docker/config.json` conserva
  `"credsStore": "desktop"` de una instalación de Docker Desktop ya borrada. Usa `podman-compose`
  o `./scripts/db.sh`. Para arreglarlo de raíz: quitar esa línea del `~/.docker/config.json`.
- `/usr/local/bin/docker` es un symlink roto a `/Applications/Docker.app` (desinstalada): por eso
  `docker` da `command not found`.
- Las imágenes se referencian como `docker.io/library/postgres:15`; podman no asume el registro.
