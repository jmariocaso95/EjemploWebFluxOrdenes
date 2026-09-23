# Checkout reactivo sobre R2DBC: caso de uso + código completo para copiar y pegar

Proyecto `demo` **real**: Spring Boot 4.1.1 · Java 17 · WebFlux · **Spring Data R2DBC + PostgreSQL** · Project Reactor · Bean Validation.
Puerto de la app: **8081** (`application.yml`). Base de datos: `docker-compose.yml` (`postgres:15`, db `testdb`).

> Versión anterior de este documento asumía MongoDB reactivo y un paquete plano `com.example.demo.Product`.
> El proyecto no usa Mongo: usa R2DBC/Postgres con `model/`, `repository/`, `service/`, `controller/` y dominio en español
> (`Cliente`, `Orden`, `Producto`). Todo el código de abajo ya está adaptado a eso.

---

## PARTE 0 · Estado real del proyecto y qué se ajusta

### Lo que ya existe

| Archivo | Qué hace |
|---|---|
| `model/Cliente.java`, `model/Orden.java`, `model/Producto.java` | Entidades R2DBC (`@Table`, `@Id`) |
| `repository/ClienteRepository`, `OrdenRepository` | `ReactiveCrudRepository` |
| `repository/ProductReactiveRepository` | **extiende `ReactiveMongoRepository`** ← incoherente con el resto |
| `service/ClienteService`, `OrdenService` | CRUD reactivo simple |
| `controller/ClienteController`, `OrdenController`, `ProductoController` | `/api/clientes`, `/api/ordenes`, `/api/products` (+ `/stream` SSE) |
| `controller/GlobalException` | `@ControllerAdvice` para `WebExchangeBindException` |
| `resources/schema.sql` | tablas `clientes`, `ordenes`, `productos` (`spring.sql.init.mode: always`) |

### Incoherencias detectadas en el proyecto (arreglar antes de empezar)

1. **`ProductReactiveRepository` es un repositorio Mongo sobre una entidad `@Table`.** No arranca contra Postgres.
   → pasa a `ReactiveCrudRepository<Producto, Long>` (bloque 2.4).
2. **`build.gradle` trae `spring-boot-starter-data-mongodb-reactive`** sin Mongo en el compose. → se elimina (bloque 2.1).
3. **Password desalineado**: `docker-compose.yml` usa `POSTGRES_PASSWORD: postgres` y `application.yml` `password: admin`.
   → se unifica en `postgres` (bloque 2.2).
4. **`ClienteControllerTest` llama `/clientes`** pero el controller mapea `/api/clientes`. → corregir la URI del test.
5. **`ProductoControllerTest` hace `save()` con `id` seteado a mano.** En R2DBC un `@Id` no nulo se interpreta como
   *update*: no inserta nada y el test queda sin datos. En Mongo sí era un upsert. → sembrar con el helper de upsert
   (bloque 2.5) o dejar que Postgres genere el `id`.
6. `OrdenController.getOrdenesByClienteId` / `saveOrden` no anotan `@PathVariable` / `@RequestBody`.
   No es parte de este caso de uso, pero está roto.

### Lo que se agrega con este caso de uso

Tablas nuevas: `orden_compra`, `orden_item`, `evento_inventario`. Columnas nuevas en `productos`: `category`, `reserved`.
La tabla `ordenes` y su `Orden`/`OrdenService` **se dejan intactas**: son la demo CRUD previa.

---

## PARTE 1 · El caso de uso

### Qué resuelve

Una tienda recibe **órdenes de compra con varios productos**. Al crear una orden el sistema debe:

1. **Validar** la petición y persistir la orden en `PENDIENTE` (Postgres asigna el `id`).
2. **Reservar stock** producto por producto con un `UPDATE ... WHERE stock >= :qty RETURNING *` (atómico por fila).
   Si falla a mitad, **compensar** (devolver lo ya reservado).
3. Consultar **en paralelo** tres servicios externos: precio dinámico (falla a veces → reintento con backoff y fallback
   al precio de catálogo), impuesto por región (lento → se cachea 10 min), antifraude (puede colgarse → timeout 800 ms
   y score por defecto).
4. **Rechazar** si el riesgo es alto (> 80) y liberar reservas.
5. Guardar orden + ítems en una **transacción R2DBC** con estado `RESERVADA` y fecha de expiración. El cliente luego la
   **confirma** (`/confirm`) y el stock reservado pasa a vendido.
6. **Emitir eventos** a un bus interno (`Sinks`) que alimenta dos streams SSE: el de una orden concreta y el tablero global.
7. Un **job** cada 30 s expira reservas vencidas y devuelve el stock.
8. Un **reporte** agrega ventas por categoría sobre miles de ítems respetando backpressure (`limitRate`).
9. **Carga masiva** de productos por NDJSON en lotes con `INSERT ... ON CONFLICT DO UPDATE`.
10. Un `correlationId` viaja por **Reactor Context** desde el header HTTP hasta los logs, sin pasarlo como parámetro.

Los servicios externos se **simulan dentro de la misma app** (`/external/**`) y se controlan con `PUT /external/simulator`
para provocar fallos, latencia o riesgo alto en la demo.

### Diferencias clave contra la versión Mongo del documento

| Tema | Mongo (antes) | R2DBC/Postgres (ahora) |
|---|---|---|
| Reserva atómica | `findAndModify` con `Criteria stock >= qty` | `UPDATE ... WHERE stock >= :qty RETURNING *` vía `DatabaseClient` |
| Ítems de la orden | lista embebida en el documento | tabla hija `orden_item` + `@Transient List<ItemOrden>` ensamblado en el service |
| Id de la orden | `UUID` generado en la app | `BIGSERIAL` de Postgres → la orden se inserta **antes** de reservar |
| Transacciones | no había | `TransactionalOperator` en persistencia de orden+ítems y en `confirm` |
| Upsert por lotes | `saveAll` con `_id` fijo | `INSERT ... ON CONFLICT (id) DO UPDATE` |
| Agregación del reporte | recorrido de documentos | `@Query` con `JOIN` + `groupBy` en Reactor |

### Elementos reactivos que se ejercitan y dónde

| Elemento | Archivo |
|---|---|
| `Mono`/`Flux`, `map`, `flatMap`, `concatMap`, `flatMapIterable` | `OrdenCompraService`, `ReservaSaga`, `ReporteService` |
| `filter`, `switchIfEmpty`, `Mono.defer`, `deferContextual` | `OrdenCompraService`, `InventarioService` |
| `Mono.zip` (paralelo), `Flux.merge` | `OrdenCompraService`, `TableroService` |
| `collectList`, `collectSortedList`, `reduce`, `scan`, `count` | `ReporteService`, `CargaMasivaService` |
| `groupBy`, `window`, `buffer` | `ReporteService`, `TableroService`, `CargaMasivaService` |
| `retryWhen(Retry.backoff)`, `timeout`, `onErrorReturn`, `onErrorResume` | `ServiciosExternosClient`, `ReactiveSupport`, `OrdenCompraService` |
| `publishOn(Schedulers.parallel())` | `OrdenCompraService` (cálculo de totales) |
| Hot publishers: `Sinks`, `cache(ttl)`, `publish().refCount` | `EventBus`, `ServiciosExternosClient`, `TableroService` |
| Backpressure: `limitRate`, `onBackpressureLatest`, `onBackpressureDrop` | `ReporteService`, `TableroService`, `ExpiracionReservasJob` |
| `Flux.interval`, `delayElement`, `takeUntil`, `distinctUntilChanged` | `ExpiracionReservasJob`, `TableroService`, simulador |
| `doOnNext`, `doOnError`, `doOnCancel`, `doFinally` | `OrdenCompraService`, `ReactiveSupport` |
| Reactor `Context` (`contextWrite` / `deferContextual`) | `CorrelationWebFilter`, `OrdenCompraService`, `GlobalErrorHandler` |
| `DatabaseClient` + `RETURNING` atómico + saga de compensación | `InventarioService`, `ReservaSaga` |
| `TransactionalOperator` reactivo | `OrdenCompraService`, `CargaMasivaService` |
| `WebClient` no bloqueante | `ServiciosExternosClient` |
| SSE (`ServerSentEvent`) y NDJSON (entrada y salida) | `OrdenCompraController`, `TableroController`, `ReporteController`, `CargaMasivaController` |
| `WebFilter`, `@RestControllerAdvice` reactivo | `CorrelationWebFilter`, `GlobalErrorHandler` |
| `Disposable` / ciclo de vida | `ExpiracionReservasJob` |
| `StepVerifier`, `withVirtualTime`, `TestPublisher`, `WebTestClient` | carpeta `src/test` |

### Endpoints (app en `localhost:8081`)

| Método | Ruta | Descripción |
|---|---|---|
| POST | `/api/orders` | Crea orden. Headers opcionales `Idempotency-Key`, `X-Correlation-Id` |
| GET | `/api/orders/{id}` | Consulta orden con sus ítems |
| POST | `/api/orders/{id}/confirm` | Confirma (reserva → venta) |
| GET | `/api/orders/{id}/events` | SSE de una orden, cierra en estado terminal |
| GET | `/api/ops/dashboard` | SSE global compartido (hot) |
| GET | `/api/reports/sales` | Totales por categoría (JSON) |
| GET | `/api/reports/sales/stream` | Total acumulado en vivo (NDJSON) |
| POST | `/api/products/bulk` | Carga NDJSON en lotes de 500 (upsert) |
| GET/PUT/DELETE | `/external/simulator` | Controla fallos/latencia de los servicios simulados |

Errores: `ProductoNoExisteException` 404 · `StockInsuficienteException` 409 · `RiesgoAltoException` 422 ·
`OrdenNoExisteException` 404 · `EstadoInvalidoException` 409 · `ValidacionException` 400.

---

## PARTE 2 · Código. Copiar cada bloque en la ruta indicada

Estructura final (respeta la convención por capas del proyecto):

```
src/main/java/com/example/demo/
├── DemoApplication.java                  (existe, no tocar)
├── common/
│   ├── AppProperties.java
│   ├── WebClientConfig.java
│   ├── CorrelationWebFilter.java
│   ├── ReactiveSupport.java
│   ├── TransientException.java
│   ├── DomainException.java
│   ├── DomainExceptions.java
│   └── GlobalErrorHandler.java
├── model/
│   ├── Cliente.java                      (existe, no tocar)
│   ├── Orden.java                        (existe, no tocar)
│   ├── Producto.java                     (REEMPLAZAR: + category, + reserved)
│   ├── EstadoOrden.java
│   ├── ItemOrden.java
│   ├── OrdenCompra.java
│   └── EventoInventario.java
├── dto/
│   ├── CrearOrdenRequest.java
│   ├── CotizacionPrecio.java
│   ├── EventoOrden.java
│   ├── EventoTablero.java
│   ├── TotalCategoria.java
│   ├── ResultadoCarga.java
│   └── SimuladorConfig.java
├── repository/
│   ├── ClienteRepository.java            (existe, no tocar)
│   ├── OrdenRepository.java              (existe, no tocar)
│   ├── ProductReactiveRepository.java    (REEMPLAZAR: Mongo → R2DBC)
│   ├── OrdenCompraRepository.java
│   ├── ItemOrdenRepository.java
│   └── EventoInventarioRepository.java
├── service/
│   ├── ClienteService.java               (existe, no tocar)
│   ├── OrdenService.java                 (existe, no tocar)
│   ├── EventBus.java
│   ├── InventarioPort.java
│   ├── InventarioService.java
│   ├── ReservaSaga.java
│   ├── ServiciosExternosPort.java
│   ├── ServiciosExternosClient.java
│   ├── OrdenCompraService.java
│   ├── TableroService.java
│   ├── ReporteService.java
│   ├── CargaMasivaService.java
│   └── ExpiracionReservasJob.java
└── controller/
    ├── ClienteController.java            (existe, no tocar)
    ├── OrdenController.java              (existe, no tocar)
    ├── ProductoController.java           (existe, no tocar)
    ├── GlobalException.java              (existe, no tocar)
    ├── OrdenCompraController.java
    ├── TableroController.java
    ├── ReporteController.java
    ├── CargaMasivaController.java
    └── SimuladorExternoController.java

src/main/resources/
├── application.yml                       (REEMPLAZAR)
└── schema.sql                            (REEMPLAZAR)

src/test/java/com/example/demo/
├── common/ReactiveSupportTest.java
├── service/EventBusTest.java
├── service/ReservaSagaTest.java
├── service/ReporteServiceTest.java
└── controller/OrdenFlowIntegrationTest.java
```

---

### 2.1 `build.gradle` — REEMPLAZAR bloque `dependencies`

Se va Mongo (no hay Mongo en el compose) y entra `reactor-test`.

```groovy
dependencies {
	implementation 'org.springframework.boot:spring-boot-starter-webflux'
	implementation 'org.springframework.boot:spring-boot-starter-data-r2dbc'
	implementation 'org.springframework.boot:spring-boot-starter-validation'
	implementation 'org.postgresql:r2dbc-postgresql'
	runtimeOnly 'org.postgresql:postgresql'          // solo para spring.sql.init (schema.sql)

	testImplementation 'org.springframework.boot:spring-boot-starter-test'
	testImplementation 'org.springframework.boot:spring-boot-starter-webflux-test'
	testImplementation 'io.projectreactor:reactor-test'
	testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
}
```

### 2.2 `src/main/resources/application.yml` — REEMPLAZAR completo

```yaml
spring:
  r2dbc:
    url: r2dbc:postgresql://localhost:5432/testdb
    username: postgres
    password: ${POSTGRES_PASSWORD:postgres}   # alineado con docker-compose.yml
  sql:
    init:
      mode: always

server:
  port: 8081

app:
  external:
    base-url: http://localhost:8081     # el simulador vive en la misma app
    pricing-timeout: 2s
    fraud-timeout: 800ms
  reservation-ttl: 2m
  expiry-interval: 30s
  risk-threshold: 80
  default-risk-score: 50
  low-stock-threshold: 5

logging:
  level:
    com.example.demo: INFO
    org.springframework.r2dbc.core: DEBUG    # muestra el SQL emitido
```

### 2.3 `src/main/resources/schema.sql` — REEMPLAZAR completo

`spring.sql.init.mode: always` lo ejecuta en cada arranque, por eso todo es `IF NOT EXISTS` / idempotente.

```sql
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
```

> `ALTER TABLE ... ADD CONSTRAINT` no acepta `IF NOT EXISTS` en Postgres 15: si reinicias con la tabla ya creada,
> el script falla en esa línea. Dos opciones: borrarla después del primer arranque, o envolverla en
> `DO $$ BEGIN ... EXCEPTION WHEN duplicate_object THEN NULL; END $$;`.

---

## model/

### 2.4 `src/main/java/com/example/demo/model/Producto.java` — REEMPLAZAR

```java
package com.example.demo.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("productos")
public class Producto {

    @Id
    private Long id;

    private String name;

    private Double price;

    private Integer stock;

    /** Categoría usada por el reporte de ventas. */
    private String category;

    /** Stock reservado y no vendido todavía. */
    private Integer reserved = 0;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public Double getPrice() { return price; }
    public void setPrice(Double price) { this.price = price; }
    public Integer getStock() { return stock; }
    public void setStock(Integer stock) { this.stock = stock; }
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
    public Integer getReserved() { return reserved; }
    public void setReserved(Integer reserved) { this.reserved = reserved; }
}
```

### 2.5 `src/main/java/com/example/demo/model/EstadoOrden.java`

```java
package com.example.demo.model;

public enum EstadoOrden {
    PENDIENTE, RESERVADA, CONFIRMADA, RECHAZADA, EXPIRADA, COMPENSADA;

    public boolean esTerminal() {
        return this == CONFIRMADA || this == RECHAZADA || this == EXPIRADA || this == COMPENSADA;
    }
}
```

> Spring Data R2DBC convierte el enum a `String` al escribir en una columna `varchar` y de vuelta al leer.
> Si el driver se queja del tipo, registra un `EnumWriteSupport<EstadoOrden>` como converter en un
> `R2dbcCustomConversions`; con `varchar` no hace falta.

### 2.6 `src/main/java/com/example/demo/model/ItemOrden.java`

En Mongo los ítems iban embebidos. En R2DBC son **tabla hija**: entidad propia con `ordenId`.

```java
package com.example.demo.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("orden_item")
public class ItemOrden {

    @Id
    private Long id;

    private Long ordenId;
    private Long productoId;
    private String categoria;
    private Integer cantidad;
    private Double precioUnitario;

    public ItemOrden() {}

    public ItemOrden(Long ordenId, Long productoId, String categoria, Integer cantidad, Double precioUnitario) {
        this.ordenId = ordenId;
        this.productoId = productoId;
        this.categoria = categoria;
        this.cantidad = cantidad;
        this.precioUnitario = precioUnitario;
    }

    /** Copia con otro precio unitario (los records serían inmutables; aquí lo hacemos explícito). */
    public ItemOrden conPrecio(Double nuevoPrecio) {
        return new ItemOrden(ordenId, productoId, categoria, cantidad, nuevoPrecio);
    }

    public ItemOrden conOrden(Long nuevaOrden) {
        return new ItemOrden(nuevaOrden, productoId, categoria, cantidad, precioUnitario);
    }

    public double totalLinea() {
        return (cantidad == null ? 0 : cantidad) * (precioUnitario == null ? 0.0 : precioUnitario);
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getOrdenId() { return ordenId; }
    public void setOrdenId(Long ordenId) { this.ordenId = ordenId; }
    public Long getProductoId() { return productoId; }
    public void setProductoId(Long productoId) { this.productoId = productoId; }
    public String getCategoria() { return categoria; }
    public void setCategoria(String categoria) { this.categoria = categoria; }
    public Integer getCantidad() { return cantidad; }
    public void setCantidad(Integer cantidad) { this.cantidad = cantidad; }
    public Double getPrecioUnitario() { return precioUnitario; }
    public void setPrecioUnitario(Double precioUnitario) { this.precioUnitario = precioUnitario; }
}
```

### 2.7 `src/main/java/com/example/demo/model/OrdenCompra.java`

```java
package com.example.demo.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Table("orden_compra")
public class OrdenCompra {

    @Id
    private Long id;

    private Long clienteId;
    private String region;
    private String idempotencyKey;
    private EstadoOrden estado;
    private Double subtotal;
    private Double impuesto;
    private Double total;
    private Integer riskScore;
    private Instant creadoEn;
    private Instant expiraEn;

    /** R2DBC no mapea relaciones: los ítems se cargan aparte y se ensamblan aquí. */
    @Transient
    private List<ItemOrden> items = new ArrayList<>();

    public OrdenCompra() {}

    public static OrdenCompra nueva(Long clienteId, String region, String idempotencyKey) {
        OrdenCompra o = new OrdenCompra();
        o.clienteId = clienteId;
        o.region = region;
        o.idempotencyKey = idempotencyKey;
        o.estado = EstadoOrden.PENDIENTE;
        o.creadoEn = Instant.now();
        return o;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getClienteId() { return clienteId; }
    public void setClienteId(Long clienteId) { this.clienteId = clienteId; }
    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public EstadoOrden getEstado() { return estado; }
    public void setEstado(EstadoOrden estado) { this.estado = estado; }
    public Double getSubtotal() { return subtotal; }
    public void setSubtotal(Double subtotal) { this.subtotal = subtotal; }
    public Double getImpuesto() { return impuesto; }
    public void setImpuesto(Double impuesto) { this.impuesto = impuesto; }
    public Double getTotal() { return total; }
    public void setTotal(Double total) { this.total = total; }
    public Integer getRiskScore() { return riskScore; }
    public void setRiskScore(Integer riskScore) { this.riskScore = riskScore; }
    public Instant getCreadoEn() { return creadoEn; }
    public void setCreadoEn(Instant creadoEn) { this.creadoEn = creadoEn; }
    public Instant getExpiraEn() { return expiraEn; }
    public void setExpiraEn(Instant expiraEn) { this.expiraEn = expiraEn; }
    public List<ItemOrden> getItems() { return items; }
    public void setItems(List<ItemOrden> items) { this.items = items == null ? new ArrayList<>() : items; }
}
```

### 2.8 `src/main/java/com/example/demo/model/EventoInventario.java`

```java
package com.example.demo.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Table("evento_inventario")
public class EventoInventario {

    @Id
    private Long id;

    private String tipo;          // RESERVADO | LIBERADO | VENDIDO
    private Long productoId;
    private Integer delta;
    private Long ordenId;
    private Instant ocurridoEn;

    public EventoInventario() {}

    public static EventoInventario de(String tipo, Long productoId, int delta, Long ordenId) {
        EventoInventario e = new EventoInventario();
        e.tipo = tipo;
        e.productoId = productoId;
        e.delta = delta;
        e.ordenId = ordenId;
        e.ocurridoEn = Instant.now();
        return e;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getTipo() { return tipo; }
    public void setTipo(String tipo) { this.tipo = tipo; }
    public Long getProductoId() { return productoId; }
    public void setProductoId(Long productoId) { this.productoId = productoId; }
    public Integer getDelta() { return delta; }
    public void setDelta(Integer delta) { this.delta = delta; }
    public Long getOrdenId() { return ordenId; }
    public void setOrdenId(Long ordenId) { this.ordenId = ordenId; }
    public Instant getOcurridoEn() { return ocurridoEn; }
    public void setOcurridoEn(Instant ocurridoEn) { this.ocurridoEn = ocurridoEn; }
}
```

---

## dto/

### 2.9 `src/main/java/com/example/demo/dto/CrearOrdenRequest.java`

```java
package com.example.demo.dto;

import java.util.List;

public record CrearOrdenRequest(Long clienteId, String region, List<Item> items) {
    public record Item(Long productoId, Integer cantidad) {}
}
```

### 2.10 `src/main/java/com/example/demo/dto/CotizacionPrecio.java`

```java
package com.example.demo.dto;

public record CotizacionPrecio(Long productoId, Double precioUnitario, String fuente) {}
```

### 2.11 `src/main/java/com/example/demo/dto/EventoOrden.java`

```java
package com.example.demo.dto;

import com.example.demo.model.EstadoOrden;

import java.time.Instant;

public record EventoOrden(Long ordenId, EstadoOrden estado, String mensaje, Instant timestamp) {

    public static EventoOrden de(Long ordenId, EstadoOrden estado, String mensaje) {
        return new EventoOrden(ordenId, estado, mensaje, Instant.now());
    }

    /** estado == null identifica un heartbeat. */
    public static EventoOrden heartbeat(Long ordenId) {
        return new EventoOrden(ordenId, null, "heartbeat", Instant.now());
    }
}
```

### 2.12 `src/main/java/com/example/demo/dto/EventoTablero.java`

```java
package com.example.demo.dto;

import java.time.Instant;

public record EventoTablero(String tipo, Object payload, Instant timestamp) {
    public static EventoTablero de(String tipo, Object payload) {
        return new EventoTablero(tipo, payload, Instant.now());
    }
}
```

### 2.13 `src/main/java/com/example/demo/dto/TotalCategoria.java`

```java
package com.example.demo.dto;

public record TotalCategoria(String categoria, long unidades, double monto) {

    public static TotalCategoria vacio(String categoria) {
        return new TotalCategoria(categoria, 0, 0.0);
    }

    public TotalCategoria mas(int cantidad, double montoLinea) {
        return new TotalCategoria(categoria, unidades + cantidad,
                Math.round((monto + montoLinea) * 100.0) / 100.0);
    }
}
```

### 2.14 `src/main/java/com/example/demo/dto/ResultadoCarga.java`

```java
package com.example.demo.dto;

public record ResultadoCarga(int guardados, int fallidos) {
    public ResultadoCarga mas(ResultadoCarga otro) {
        return new ResultadoCarga(guardados + otro.guardados, fallidos + otro.fallidos);
    }
}
```

### 2.15 `src/main/java/com/example/demo/dto/SimuladorConfig.java`

```java
package com.example.demo.dto;

/** Vista/patch de las perillas del simulador. Campos null = no cambiar. */
public record SimuladorConfig(Integer pricingFailures, Long fraudDelayMs, Integer forcedRiskScore) {}
```

---

## common/

### 2.16 `src/main/java/com/example/demo/common/AppProperties.java`

```java
package com.example.demo.common;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("app")
public record AppProperties(
        External external,
        Duration reservationTtl,
        Duration expiryInterval,
        int riskThreshold,
        int defaultRiskScore,
        int lowStockThreshold) {

    public record External(String baseUrl, Duration pricingTimeout, Duration fraudTimeout) {}
}
```

### 2.17 `src/main/java/com/example/demo/common/WebClientConfig.java`

También expone el `TransactionalOperator` que usan los services.

```java
package com.example.demo.common;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
@EnableConfigurationProperties(AppProperties.class)
public class WebClientConfig {

    @Bean
    public WebClient externalWebClient(AppProperties props) {
        return WebClient.builder()
                .baseUrl(props.external().baseUrl())
                .build();
    }

    @Bean
    public ReactiveTransactionManager transactionManager(ConnectionFactory connectionFactory) {
        return new R2dbcTransactionManager(connectionFactory);
    }

    @Bean
    public TransactionalOperator transactionalOperator(ReactiveTransactionManager tm) {
        return TransactionalOperator.create(tm);
    }
}
```

### 2.18 `src/main/java/com/example/demo/common/CorrelationWebFilter.java`

```java
package com.example.demo.common;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.UUID;

/**
 * Lee X-Correlation-Id (o genera uno) y lo pone en el Reactor Context.
 * Cualquier operador aguas abajo lo recupera con Mono.deferContextual sin recibirlo como parámetro.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationWebFilter implements WebFilter {

    public static final String KEY = "correlationId";
    public static final String HEADER = "X-Correlation-Id";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String cid = Optional.ofNullable(exchange.getRequest().getHeaders().getFirst(HEADER))
                .orElse(UUID.randomUUID().toString());
        exchange.getResponse().getHeaders().add(HEADER, cid);
        return chain.filter(exchange).contextWrite(ctx -> ctx.put(KEY, cid));
    }
}
```

### 2.19 `src/main/java/com/example/demo/common/TransientException.java`

```java
package com.example.demo.common;

public class TransientException extends RuntimeException {
    public TransientException(String message) { super(message); }
}
```

### 2.20 `src/main/java/com/example/demo/common/ReactiveSupport.java`

Igual que en la versión Mongo, más los errores transitorios de R2DBC (`TransientDataAccessException`:
deadlock, serialización, conexión caída → sí se reintentan).

```java
package com.example.demo.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public final class ReactiveSupport {

    private static final Logger log = LoggerFactory.getLogger(ReactiveSupport.class);

    private ReactiveSupport() {}

    /** Backoff exponencial con jitter. Solo reintenta errores transitorios. Al agotar, propaga el error original. */
    public static Retry backoff(int attempts, Duration firstDelay) {
        return Retry.backoff(attempts, firstDelay)
                .jitter(0.5)
                .filter(ReactiveSupport::isTransient)
                .doBeforeRetry(s -> log.warn("Reintento #{} por {}", s.totalRetries() + 1, s.failure().toString()))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }

    public static boolean isTransient(Throwable t) {
        return t instanceof TransientException
                || t instanceof TimeoutException
                || t instanceof TransientDataAccessException
                || t instanceof WebClientRequestException
                || (t instanceof WebClientResponseException w && w.getStatusCode().is5xxServerError());
    }

    /** Envuelve un paso con logs que incluyen el correlationId tomado del Context. */
    public static <T> Mono<T> traced(String step, Mono<T> source) {
        return Mono.deferContextual(ctx -> {
            String cid = ctx.getOrDefault(CorrelationWebFilter.KEY, "n/a");
            return source
                    .doOnSubscribe(s -> log.info("[{}] {} → inicio", cid, step))
                    .doOnSuccess(v -> log.info("[{}] {} → ok", cid, step))
                    .doOnError(e -> log.warn("[{}] {} → error {}", cid, step, e.toString()))
                    .doFinally(sig -> log.debug("[{}] {} → fin ({})", cid, step, sig));
        });
    }
}
```

### 2.21 `src/main/java/com/example/demo/common/DomainException.java`

```java
package com.example.demo.common;

import org.springframework.http.HttpStatus;

public abstract class DomainException extends RuntimeException {

    private final HttpStatus status;

    protected DomainException(HttpStatus status, String message) {
        super(message);
        this.status = status;
    }

    public HttpStatus status() { return status; }
}
```

### 2.22 `src/main/java/com/example/demo/common/DomainExceptions.java`

```java
package com.example.demo.common;

import org.springframework.http.HttpStatus;

public final class DomainExceptions {

    private DomainExceptions() {}

    public static class ValidacionException extends DomainException {
        public ValidacionException(String msg) { super(HttpStatus.BAD_REQUEST, msg); }
    }

    public static class ProductoNoExisteException extends DomainException {
        public ProductoNoExisteException(Long id) { super(HttpStatus.NOT_FOUND, "Producto no existe: " + id); }
    }

    public static class StockInsuficienteException extends DomainException {
        public StockInsuficienteException(Long id, int cantidad) {
            super(HttpStatus.CONFLICT, "Stock insuficiente para producto " + id + " (cantidad " + cantidad + ")");
        }
    }

    public static class RiesgoAltoException extends DomainException {
        public RiesgoAltoException(int score) { super(HttpStatus.UNPROCESSABLE_ENTITY, "Riesgo alto: " + score); }
    }

    public static class OrdenNoExisteException extends DomainException {
        public OrdenNoExisteException(Long id) { super(HttpStatus.NOT_FOUND, "Orden no existe: " + id); }
    }

    public static class EstadoInvalidoException extends DomainException {
        public EstadoInvalidoException(String msg) { super(HttpStatus.CONFLICT, msg); }
    }
}
```

### 2.23 `src/main/java/com/example/demo/common/GlobalErrorHandler.java`

Convive con el `GlobalException` que ya existe: ese maneja `WebExchangeBindException` (validación de `@Valid`),
este maneja el dominio. Spring elige el handler más específico por tipo de excepción.

```java
package com.example.demo.common;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;

@RestControllerAdvice
public class GlobalErrorHandler {

    @ExceptionHandler(DomainException.class)
    public Mono<ResponseEntity<Map<String, Object>>> domain(DomainException ex) {
        return Mono.deferContextual(ctx -> {
            Map<String, Object> body = Map.of(
                    "error", ex.getClass().getSimpleName(),
                    "message", ex.getMessage(),
                    "correlationId", ctx.getOrDefault(CorrelationWebFilter.KEY, "n/a"),
                    "timestamp", Instant.now().toString());
            return Mono.just(ResponseEntity.status(ex.status()).body(body));
        });
    }
}
```

---

## repository/

### 2.24 `src/main/java/com/example/demo/repository/ProductReactiveRepository.java` — REEMPLAZAR

Era `ReactiveMongoRepository`. Pasa a R2DBC y se le agrega la consulta de stock bajo.

```java
package com.example.demo.repository;

import com.example.demo.model.Producto;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface ProductReactiveRepository extends ReactiveCrudRepository<Producto, Long> {

    Flux<Producto> findByStockLessThan(int threshold);
}
```

### 2.25 `src/main/java/com/example/demo/repository/OrdenCompraRepository.java`

```java
package com.example.demo.repository;

import com.example.demo.model.EstadoOrden;
import com.example.demo.model.OrdenCompra;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

public interface OrdenCompraRepository extends ReactiveCrudRepository<OrdenCompra, Long> {

    Mono<OrdenCompra> findByIdempotencyKey(String idempotencyKey);

    Flux<OrdenCompra> findByEstado(EstadoOrden estado);

    Flux<OrdenCompra> findByEstadoAndExpiraEnBefore(EstadoOrden estado, Instant before);
}
```

### 2.26 `src/main/java/com/example/demo/repository/ItemOrdenRepository.java`

```java
package com.example.demo.repository;

import com.example.demo.model.ItemOrden;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface ItemOrdenRepository extends ReactiveCrudRepository<ItemOrden, Long> {

    Flux<ItemOrden> findByOrdenId(Long ordenId);

    /** El JOIN reemplaza la lista embebida de Mongo. Se consume en streaming, no se materializa. */
    @Query("""
           SELECT i.* FROM orden_item i
           JOIN orden_compra o ON o.id = i.orden_id
           WHERE o.estado = :estado
           """)
    Flux<ItemOrden> findByEstadoOrden(String estado);
}
```

### 2.27 `src/main/java/com/example/demo/repository/EventoInventarioRepository.java`

```java
package com.example.demo.repository;

import com.example.demo.model.EventoInventario;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface EventoInventarioRepository extends ReactiveCrudRepository<EventoInventario, Long> {
}
```

---

## service/

### 2.28 `src/main/java/com/example/demo/service/EventBus.java`

```java
package com.example.demo.service;

import com.example.demo.dto.EventoOrden;
import com.example.demo.model.EventoInventario;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Bus in-memory. Hot publishers:
 *  - ordenes: replay de los últimos 200 eventos → un suscriptor tardío ve historia reciente.
 *  - inventario: directBestEffort → si nadie escucha se descarta, nunca bloquea al productor.
 */
@Component
public class EventBus {

    private final Sinks.Many<EventoOrden> ordenes = Sinks.many().replay().limit(200);
    private final Sinks.Many<EventoInventario> inventario = Sinks.many().multicast().directBestEffort();

    public void publicar(EventoOrden evento) {
        ordenes.emitNext(evento, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    public void publicar(EventoInventario evento) {
        inventario.emitNext(evento, Sinks.EmitFailureHandler.FAIL_FAST);
    }

    public Flux<EventoOrden> eventosOrden() { return ordenes.asFlux(); }

    public Flux<EventoInventario> eventosInventario() { return inventario.asFlux(); }
}
```

### 2.29 `src/main/java/com/example/demo/service/InventarioPort.java`

```java
package com.example.demo.service;

import com.example.demo.model.Producto;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface InventarioPort {
    Mono<Producto> reservar(Long productoId, int cantidad, Long ordenId);
    Mono<Void> liberar(Long productoId, int cantidad, Long ordenId);
    Mono<Void> vender(Long productoId, int cantidad, Long ordenId);
    Flux<Producto> stockBajo(int threshold);
}
```

### 2.30 `src/main/java/com/example/demo/service/InventarioService.java`

Núcleo del cambio contra Mongo: **un solo `UPDATE` condicional con `RETURNING`**. Postgres bloquea la fila
durante el update, así que dos órdenes compitiendo por el último ítem no pueden reservar las dos.
`.one()` devuelve `Mono.empty()` si el `WHERE` no encontró fila → ahí se decide si es *no existe* o *sin stock*.

```java
package com.example.demo.service;

import com.example.demo.common.DomainExceptions.ProductoNoExisteException;
import com.example.demo.common.DomainExceptions.StockInsuficienteException;
import com.example.demo.model.EventoInventario;
import com.example.demo.model.Producto;
import com.example.demo.repository.EventoInventarioRepository;
import com.example.demo.repository.ProductReactiveRepository;
import io.r2dbc.spi.Readable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class InventarioService implements InventarioPort {

    private static final String SQL_RESERVAR = """
            UPDATE productos
               SET stock = stock - :cantidad,
                   reserved = reserved + :cantidad
             WHERE id = :id AND stock >= :cantidad
            RETURNING id, name, price, stock, reserved, category
            """;

    private static final String SQL_LIBERAR = """
            UPDATE productos
               SET stock = stock + :cantidad,
                   reserved = GREATEST(reserved - :cantidad, 0)
             WHERE id = :id
            """;

    private static final String SQL_VENDER = """
            UPDATE productos
               SET reserved = GREATEST(reserved - :cantidad, 0)
             WHERE id = :id
            """;

    private final DatabaseClient db;
    private final ProductReactiveRepository productos;
    private final EventoInventarioRepository eventos;
    private final EventBus bus;

    public InventarioService(DatabaseClient db, ProductReactiveRepository productos,
                             EventoInventarioRepository eventos, EventBus bus) {
        this.db = db;
        this.productos = productos;
        this.eventos = eventos;
        this.bus = bus;
    }

    @Override
    public Mono<Producto> reservar(Long productoId, int cantidad, Long ordenId) {
        return db.sql(SQL_RESERVAR)
                .bind("cantidad", cantidad)
                .bind("id", productoId)
                .map(InventarioService::aProducto)
                .one()
                .switchIfEmpty(Mono.defer(() -> productos.existsById(productoId)
                        .flatMap(existe -> Mono.<Producto>error(existe
                                ? new StockInsuficienteException(productoId, cantidad)
                                : new ProductoNoExisteException(productoId)))))
                .flatMap(p -> registrar("RESERVADO", productoId, -cantidad, ordenId).thenReturn(p));
    }

    @Override
    public Mono<Void> liberar(Long productoId, int cantidad, Long ordenId) {
        return db.sql(SQL_LIBERAR)
                .bind("cantidad", cantidad)
                .bind("id", productoId)
                .fetch().rowsUpdated()
                .then(registrar("LIBERADO", productoId, cantidad, ordenId));
    }

    @Override
    public Mono<Void> vender(Long productoId, int cantidad, Long ordenId) {
        return db.sql(SQL_VENDER)
                .bind("cantidad", cantidad)
                .bind("id", productoId)
                .fetch().rowsUpdated()
                .then(registrar("VENDIDO", productoId, 0, ordenId));
    }

    @Override
    public Flux<Producto> stockBajo(int threshold) {
        return productos.findByStockLessThan(threshold);
    }

    private Mono<Void> registrar(String tipo, Long productoId, int delta, Long ordenId) {
        return eventos.save(EventoInventario.de(tipo, productoId, delta, ordenId))
                .doOnNext(bus::publicar)
                .then();
    }

    private static Producto aProducto(Readable row) {
        Producto p = new Producto();
        p.setId(row.get("id", Long.class));
        p.setName(row.get("name", String.class));
        java.math.BigDecimal price = row.get("price", java.math.BigDecimal.class);
        p.setPrice(price == null ? null : price.doubleValue());
        p.setStock(row.get("stock", Integer.class));
        p.setReserved(row.get("reserved", Integer.class));
        p.setCategory(row.get("category", String.class));
        return p;
    }
}
```

### 2.31 `src/main/java/com/example/demo/service/ReservaSaga.java`

```java
package com.example.demo.service;

import com.example.demo.model.ItemOrden;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Reserva secuencial (concatMap) y compensación de lo ya reservado si algo falla.
 * Cada reserva es su propia transacción de fila: la consistencia entre productos la da esta saga,
 * no una transacción larga que mantendría filas bloqueadas mientras se llama a servicios externos.
 */
@Component
public class ReservaSaga {

    private static final Logger log = LoggerFactory.getLogger(ReservaSaga.class);

    private final InventarioPort inventario;

    public ReservaSaga(InventarioPort inventario) {
        this.inventario = inventario;
    }

    /** Devuelve los ítems enriquecidos con categoría y precio de catálogo. */
    public Mono<List<ItemOrden>> reservarTodo(List<ItemOrden> items, Long ordenId) {
        return Mono.defer(() -> {
            List<ItemOrden> hechos = new CopyOnWriteArrayList<>();
            return Flux.fromIterable(items)
                    .concatMap(i -> inventario.reservar(i.getProductoId(), i.getCantidad(), ordenId)
                            .map(p -> new ItemOrden(ordenId, i.getProductoId(), p.getCategory(),
                                    i.getCantidad(), p.getPrice())))
                    .doOnNext(hechos::add)
                    .collectList()
                    .onErrorResume(ex -> {
                        log.warn("Reserva fallida en orden {}: {}. Compensando {} ítems",
                                ordenId, ex.getMessage(), hechos.size());
                        return liberarTodo(hechos, ordenId).then(Mono.error(ex));
                    });
        });
    }

    public Mono<Void> liberarTodo(List<ItemOrden> items, Long ordenId) {
        if (items == null || items.isEmpty()) return Mono.empty();
        return Flux.fromIterable(items)
                .concatMap(i -> inventario.liberar(i.getProductoId(), i.getCantidad(), ordenId)
                        .onErrorResume(e -> {
                            log.error("No se pudo liberar producto {} de orden {}: {}",
                                    i.getProductoId(), ordenId, e.toString());
                            return Mono.empty();
                        }))
                .then();
    }
}
```

### 2.32 `src/main/java/com/example/demo/service/ServiciosExternosPort.java`

```java
package com.example.demo.service;

import com.example.demo.dto.CotizacionPrecio;
import reactor.core.publisher.Mono;

public interface ServiciosExternosPort {
    Mono<CotizacionPrecio> precio(Long productoId);
    Mono<Double> tasaImpuesto(String region);
    Mono<Integer> scoreRiesgo(String clienteId, double totalEstimado);
}
```

### 2.33 `src/main/java/com/example/demo/service/ServiciosExternosClient.java`

```java
package com.example.demo.service;

import com.example.demo.common.AppProperties;
import com.example.demo.common.ReactiveSupport;
import com.example.demo.dto.CotizacionPrecio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

@Component
public class ServiciosExternosClient implements ServiciosExternosPort {

    private static final Logger log = LoggerFactory.getLogger(ServiciosExternosClient.class);

    private final WebClient web;
    private final AppProperties props;
    private final Map<String, Mono<Double>> cacheImpuesto = new ConcurrentHashMap<>();

    public ServiciosExternosClient(WebClient externalWebClient, AppProperties props) {
        this.web = externalWebClient;
        this.props = props;
    }

    /** Timeout + retry con backoff. El fallback al catálogo lo decide OrdenCompraService. */
    @Override
    public Mono<CotizacionPrecio> precio(Long productoId) {
        return web.get().uri("/external/pricing/{id}", productoId)
                .retrieve()
                .bodyToMono(CotizacionPrecio.class)
                .timeout(props.external().pricingTimeout())
                .retryWhen(ReactiveSupport.backoff(3, Duration.ofMillis(200)));
    }

    /** Memoiza por región 10 min. Los errores no se cachean (ttl error = 0). */
    @Override
    public Mono<Double> tasaImpuesto(String region) {
        String key = region == null ? "DEFAULT" : region.toUpperCase();
        return cacheImpuesto.computeIfAbsent(key, r -> web.get().uri("/external/tax/{r}", r)
                .retrieve()
                .bodyToMono(Double.class)
                .doOnNext(rate -> log.info("Tasa de impuesto {} = {} (cacheada 10 min)", r, rate))
                .cache(Duration.ofMinutes(10), Duration.ZERO, Duration.ZERO));
    }

    /** Timeout corto y degradación a score por defecto. */
    @Override
    public Mono<Integer> scoreRiesgo(String clienteId, double totalEstimado) {
        return web.post().uri("/external/fraud")
                .bodyValue(Map.of("clienteId", clienteId, "total", totalEstimado))
                .retrieve()
                .bodyToMono(Integer.class)
                .timeout(props.external().fraudTimeout())
                .doOnError(TimeoutException.class,
                        e -> log.warn("Antifraude excedió {}; usando score por defecto", props.external().fraudTimeout()))
                .onErrorReturn(TimeoutException.class, props.defaultRiskScore());
    }
}
```

### 2.34 `src/main/java/com/example/demo/service/OrdenCompraService.java`

Diferencia estructural contra Mongo: la orden se **inserta primero** en `PENDIENTE` para que Postgres asigne el `id`
(ese `id` es el que viaja a la saga y a los eventos), y los ítems se guardan en su tabla dentro de una
transacción junto con el `UPDATE` de la orden.

```java
package com.example.demo.service;

import com.example.demo.common.AppProperties;
import com.example.demo.common.CorrelationWebFilter;
import com.example.demo.common.DomainExceptions.EstadoInvalidoException;
import com.example.demo.common.DomainExceptions.OrdenNoExisteException;
import com.example.demo.common.DomainExceptions.RiesgoAltoException;
import com.example.demo.common.DomainExceptions.ValidacionException;
import com.example.demo.common.ReactiveSupport;
import com.example.demo.dto.CrearOrdenRequest;
import com.example.demo.dto.EventoOrden;
import com.example.demo.model.EstadoOrden;
import com.example.demo.model.ItemOrden;
import com.example.demo.model.OrdenCompra;
import com.example.demo.repository.ItemOrdenRepository;
import com.example.demo.repository.OrdenCompraRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class OrdenCompraService {

    private static final Logger log = LoggerFactory.getLogger(OrdenCompraService.class);

    private final OrdenCompraRepository ordenes;
    private final ItemOrdenRepository items;
    private final ReservaSaga saga;
    private final InventarioPort inventario;
    private final ServiciosExternosPort externos;
    private final EventBus bus;
    private final AppProperties props;
    private final TransactionalOperator tx;

    public OrdenCompraService(OrdenCompraRepository ordenes, ItemOrdenRepository items, ReservaSaga saga,
                              InventarioPort inventario, ServiciosExternosPort externos, EventBus bus,
                              AppProperties props, TransactionalOperator tx) {
        this.ordenes = ordenes;
        this.items = items;
        this.saga = saga;
        this.inventario = inventario;
        this.externos = externos;
        this.bus = bus;
        this.props = props;
        this.tx = tx;
    }

    // ---------- Crear ----------

    public Mono<OrdenCompra> crear(CrearOrdenRequest req, String idempotencyKey) {
        return validar(req)
                .then(buscarExistente(idempotencyKey))
                .switchIfEmpty(Mono.defer(() -> crearNueva(req, idempotencyKey)));
    }

    private Mono<Void> validar(CrearOrdenRequest req) {
        if (req == null || req.items() == null || req.items().isEmpty()) {
            return Mono.error(new ValidacionException("La orden debe tener al menos un ítem"));
        }
        if (req.clienteId() == null) {
            return Mono.error(new ValidacionException("clienteId es obligatorio"));
        }
        boolean cantidadMala = req.items().stream()
                .anyMatch(i -> i.productoId() == null || i.cantidad() == null || i.cantidad() <= 0);
        if (cantidadMala) {
            return Mono.error(new ValidacionException("Cada ítem requiere productoId y cantidad > 0"));
        }
        return Mono.empty();
    }

    private Mono<OrdenCompra> buscarExistente(String key) {
        if (key == null || key.isBlank()) return Mono.empty();
        return ordenes.findByIdempotencyKey(key)
                .doOnNext(o -> log.info("Idempotency-Key {} ya procesada → orden {}", key, o.getId()))
                .flatMap(this::conItems);
    }

    private Mono<OrdenCompra> crearNueva(CrearOrdenRequest req, String key) {
        AtomicReference<List<ItemOrden>> reservados = new AtomicReference<>(List.of());

        Mono<OrdenCompra> flujo = ordenes.save(OrdenCompra.nueva(req.clienteId(), req.region(), key))
                .flatMap(orden -> {
                    List<ItemOrden> pedidos = req.items().stream()
                            .map(i -> new ItemOrden(orden.getId(), i.productoId(), null, i.cantidad(), null))
                            .toList();

                    return ReactiveSupport.traced("reserva", saga.reservarTodo(pedidos, orden.getId()))
                            .doOnNext(reservados::set)
                            .doOnNext(reservadosOk -> {
                                orden.setItems(reservadosOk);
                                orden.setEstado(EstadoOrden.RESERVADA);
                                orden.setExpiraEn(Instant.now().plus(props.reservationTtl()));
                            })
                            .flatMap(reservadosOk -> ReactiveSupport.traced("precios+impuesto+fraude",
                                    tarificar(orden, reservadosOk)))
                            .flatMap(o -> o.getRiskScore() > props.riskThreshold()
                                    ? Mono.<OrdenCompra>error(new RiesgoAltoException(o.getRiskScore()))
                                    : Mono.just(o))
                            .flatMap(this::persistir)
                            .doOnNext(o -> bus.publicar(EventoOrden.de(o.getId(), o.getEstado(),
                                    "Stock reservado, total " + o.getTotal())))
                            .onErrorResume(ex -> compensar(orden, reservados.get(), ex));
                });

        return Mono.deferContextual(ctx -> {
            String cid = ctx.getOrDefault(CorrelationWebFilter.KEY, "n/a");
            return flujo
                    .doOnSubscribe(s -> log.info("[{}] Creando orden para cliente {}", cid, req.clienteId()))
                    .doOnError(e -> log.warn("[{}] Orden falló: {}", cid, e.toString()))
                    .doFinally(sig -> log.info("[{}] Flujo de creación terminó con señal {}", cid, sig));
        });
    }

    /** Tres llamadas externas en paralelo con Mono.zip; cálculo CPU en Schedulers.parallel(). */
    private Mono<OrdenCompra> tarificar(OrdenCompra orden, List<ItemOrden> reservados) {
        double estimado = reservados.stream().mapToDouble(ItemOrden::totalLinea).sum();

        Mono<List<ItemOrden>> conPrecio = Flux.fromIterable(reservados)
                .flatMap(i -> externos.precio(i.getProductoId())
                        .map(q -> i.conPrecio(q.precioUnitario()))
                        .onErrorResume(ex -> {
                            log.warn("Precio dinámico no disponible para {} ({}); fallback catálogo",
                                    i.getProductoId(), ex.getClass().getSimpleName());
                            return Mono.just(i);
                        }), 8)
                .collectList();

        Mono<Double> tasa = externos.tasaImpuesto(orden.getRegion());
        Mono<Integer> riesgo = externos.scoreRiesgo(String.valueOf(orden.getClienteId()), estimado);

        return Mono.zip(conPrecio, tasa, riesgo)
                .publishOn(Schedulers.parallel())
                .map(t -> {
                    List<ItemOrden> tarificados = t.getT1();
                    double subtotal = redondear(tarificados.stream().mapToDouble(ItemOrden::totalLinea).sum());
                    double impuesto = redondear(subtotal * t.getT2());
                    orden.setItems(tarificados);
                    orden.setSubtotal(subtotal);
                    orden.setImpuesto(impuesto);
                    orden.setTotal(redondear(subtotal + impuesto));
                    orden.setRiskScore(t.getT3());
                    return orden;
                });
    }

    /** Ítems + cabecera en una sola transacción: o queda todo o no queda nada. */
    private Mono<OrdenCompra> persistir(OrdenCompra orden) {
        List<ItemOrden> aGuardar = orden.getItems().stream()
                .map(i -> i.conOrden(orden.getId()))
                .toList();

        Mono<OrdenCompra> escritura = items.deleteAll(items.findByOrdenId(orden.getId()))
                .thenMany(items.saveAll(aGuardar))
                .collectList()
                .flatMap(guardados -> ordenes.save(orden).doOnNext(o -> o.setItems(guardados)));

        return escritura.as(tx::transactional);
    }

    /** Si ya había reserva, la devuelve, persiste el estado final y re-emite el error original. */
    private Mono<OrdenCompra> compensar(OrdenCompra orden, List<ItemOrden> reservados, Throwable ex) {
        EstadoOrden estadoFinal = ex instanceof RiesgoAltoException ? EstadoOrden.RECHAZADA : EstadoOrden.COMPENSADA;
        return saga.liberarTodo(reservados, orden.getId())
                .then(Mono.defer(() -> {
                    orden.setEstado(estadoFinal);
                    orden.setExpiraEn(null);
                    return ordenes.save(orden);
                }))
                .doOnNext(o -> bus.publicar(EventoOrden.de(o.getId(), estadoFinal, ex.getMessage())))
                .then(Mono.error(ex));
    }

    // ---------- Consultar / confirmar ----------

    public Mono<OrdenCompra> obtener(Long id) {
        return ordenes.findById(id)
                .switchIfEmpty(Mono.error(new OrdenNoExisteException(id)))
                .flatMap(this::conItems);
    }

    private Mono<OrdenCompra> conItems(OrdenCompra orden) {
        return items.findByOrdenId(orden.getId())
                .collectList()
                .doOnNext(orden::setItems)
                .thenReturn(orden);
    }

    public Mono<OrdenCompra> confirmar(Long id) {
        return obtener(id)
                .flatMap(o -> o.getEstado() != EstadoOrden.RESERVADA
                        ? Mono.<OrdenCompra>error(new EstadoInvalidoException("La orden está en " + o.getEstado()))
                        : Flux.fromIterable(o.getItems())
                                .concatMap(i -> inventario.vender(i.getProductoId(), i.getCantidad(), id))
                                .then(Mono.defer(() -> {
                                    o.setEstado(EstadoOrden.CONFIRMADA);
                                    o.setExpiraEn(null);
                                    return ordenes.save(o);
                                }))
                                .doOnNext(saved -> saved.setItems(o.getItems()))
                                .as(tx::transactional))
                .doOnNext(o -> bus.publicar(EventoOrden.de(o.getId(), o.getEstado(), "Orden confirmada")));
    }

    // ---------- Stream por orden ----------

    public Flux<EventoOrden> eventos(Long id) {
        Flux<EventoOrden> heartbeat = Flux.interval(Duration.ofSeconds(15)).map(t -> EventoOrden.heartbeat(id));
        return obtener(id).flatMapMany(o -> {
            Mono<EventoOrden> actual = Mono.just(EventoOrden.de(o.getId(), o.getEstado(), "estado actual"));
            Flux<EventoOrden> vivo = bus.eventosOrden().filter(e -> id.equals(e.ordenId()));
            return Flux.merge(actual, vivo, heartbeat)
                    .takeUntil(e -> e.estado() != null && e.estado().esTerminal())
                    .doOnCancel(() -> log.info("Cliente cerró stream de orden {}", id));
        });
    }

    private static double redondear(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
```

### 2.35 `src/main/java/com/example/demo/service/TableroService.java`

```java
package com.example.demo.service;

import com.example.demo.common.AppProperties;
import com.example.demo.dto.EventoTablero;
import com.example.demo.model.Producto;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;

/**
 * Un solo Flux compartido entre todos los operadores (publish().refCount).
 * Se enciende con el primer suscriptor y se apaga 5 s después del último.
 */
@Service
public class TableroService {

    private final Flux<EventoTablero> compartido;

    public TableroService(EventBus bus, InventarioPort inventario, AppProperties props) {
        Flux<EventoTablero> deOrdenes = bus.eventosOrden()
                .map(e -> EventoTablero.de("order", e));

        Flux<EventoTablero> deInventario = bus.eventosInventario()
                .map(e -> EventoTablero.de("inventory", e));

        Flux<EventoTablero> stockBajo = Flux.interval(Duration.ZERO, Duration.ofSeconds(10))
                .onBackpressureDrop()
                .concatMap(t -> inventario.stockBajo(props.lowStockThreshold()).map(Producto::getId).collectList())
                .distinctUntilChanged()
                .filter(ids -> !ids.isEmpty())
                .map(ids -> EventoTablero.de("low-stock", ids));

        Flux<EventoTablero> porVentana = bus.eventosOrden()
                .window(Duration.ofSeconds(30))
                .flatMap(Flux::count)
                .map(c -> EventoTablero.de("orders-per-30s", c));

        this.compartido = Flux.merge(deOrdenes, deInventario, stockBajo, porVentana)
                .publish()
                .refCount(1, Duration.ofSeconds(5));
    }

    /** Cliente lento recibe el último evento, no una cola infinita. */
    public Flux<EventoTablero> stream() {
        return compartido.onBackpressureLatest();
    }
}
```

### 2.36 `src/main/java/com/example/demo/service/ReporteService.java`

```java
package com.example.demo.service;

import com.example.demo.dto.TotalCategoria;
import com.example.demo.model.EstadoOrden;
import com.example.demo.model.ItemOrden;
import com.example.demo.repository.ItemOrdenRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;

@Service
public class ReporteService {

    private final ItemOrdenRepository items;

    public ReporteService(ItemOrdenRepository items) {
        this.items = items;
    }

    /** limitRate(256): el driver R2DBC pide filas en lotes; nunca se materializa el resultado completo. */
    public Mono<List<TotalCategoria>> ventasPorCategoria() {
        return agregar(itemsConfirmados());
    }

    /** scan: total acumulado corriendo, emitido como NDJSON. */
    public Flux<TotalCategoria> totalCorriendo() {
        return itemsConfirmados()
                .scan(TotalCategoria.vacio("TOTAL"), (acc, i) -> acc.mas(i.getCantidad(), i.totalLinea()))
                .skip(1);
    }

    private Flux<ItemOrden> itemsConfirmados() {
        return items.findByEstadoOrden(EstadoOrden.CONFIRMADA.name())
                .limitRate(256);
    }

    /** Pura, sin I/O: fácil de probar con StepVerifier. */
    public Mono<List<TotalCategoria>> agregar(Flux<ItemOrden> fuente) {
        return fuente
                .groupBy(i -> i.getCategoria() == null ? "SIN_CATEGORIA" : i.getCategoria())
                .flatMap(grupo -> grupo.reduce(TotalCategoria.vacio(grupo.key()),
                        (acc, i) -> acc.mas(i.getCantidad(), i.totalLinea())))
                .collectSortedList(Comparator.comparing(TotalCategoria::categoria));
    }
}
```

### 2.37 `src/main/java/com/example/demo/service/CargaMasivaService.java`

En Mongo `saveAll` con `_id` fijo era un upsert. En R2DBC un `@Id` no nulo genera `UPDATE` y no inserta nada,
así que el lote se escribe con `INSERT ... ON CONFLICT (id) DO UPDATE`, cada lote en su transacción.

```java
package com.example.demo.service;

import com.example.demo.dto.ResultadoCarga;
import com.example.demo.model.Producto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
public class CargaMasivaService {

    private static final Logger log = LoggerFactory.getLogger(CargaMasivaService.class);

    private static final String UPSERT = """
            INSERT INTO productos (id, name, price, stock, category, reserved)
            VALUES (:id, :name, :price, :stock, :category, 0)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                price = EXCLUDED.price,
                stock = EXCLUDED.stock,
                category = EXCLUDED.category
            """;

    private final DatabaseClient db;
    private final TransactionalOperator tx;

    public CargaMasivaService(DatabaseClient db, TransactionalOperator tx) {
        this.db = db;
        this.tx = tx;
    }

    /** Entrada en streaming, lotes de 500, máximo 2 lotes concurrentes. */
    public Mono<ResultadoCarga> cargar(Flux<Producto> productos) {
        return productos
                .buffer(500)
                .flatMap(lote -> guardarLote(lote)
                        .map(n -> new ResultadoCarga(n, 0))
                        .onErrorResume(e -> {
                            log.error("Lote de {} productos falló: {}", lote.size(), e.toString());
                            return Mono.just(new ResultadoCarga(0, lote.size()));
                        }), 2)
                .reduce(new ResultadoCarga(0, 0), ResultadoCarga::mas);
    }

    private Mono<Integer> guardarLote(List<Producto> lote) {
        return Flux.fromIterable(lote)
                .concatMap(p -> db.sql(UPSERT)
                        .bind("id", p.getId())
                        .bind("name", p.getName())
                        .bind("price", p.getPrice())
                        .bind("stock", p.getStock())
                        .bind("category", p.getCategory() == null ? "SIN_CATEGORIA" : p.getCategory())
                        .fetch().rowsUpdated())
                .reduce(0L, Long::sum)
                .map(Long::intValue)
                .as(tx::transactional);
    }
}
```

### 2.38 `src/main/java/com/example/demo/service/ExpiracionReservasJob.java`

```java
package com.example.demo.service;

import com.example.demo.common.AppProperties;
import com.example.demo.dto.EventoOrden;
import com.example.demo.model.EstadoOrden;
import com.example.demo.repository.ItemOrdenRepository;
import com.example.demo.repository.OrdenCompraRepository;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Flux.interval como reloj. onBackpressureDrop evita solapar ticks si uno tarda.
 * Un error de una orden no mata el job; un error del tick tampoco (onErrorResume + retry).
 */
@Component
public class ExpiracionReservasJob {

    private static final Logger log = LoggerFactory.getLogger(ExpiracionReservasJob.class);

    private final OrdenCompraRepository ordenes;
    private final ItemOrdenRepository items;
    private final ReservaSaga saga;
    private final EventBus bus;
    private final AppProperties props;
    private Disposable suscripcion;

    public ExpiracionReservasJob(OrdenCompraRepository ordenes, ItemOrdenRepository items,
                                 ReservaSaga saga, EventBus bus, AppProperties props) {
        this.ordenes = ordenes;
        this.items = items;
        this.saga = saga;
        this.bus = bus;
        this.props = props;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void iniciar() {
        suscripcion = Flux.interval(props.expiryInterval())
                .onBackpressureDrop(t -> log.warn("Tick {} descartado: el anterior aún corre", t))
                .concatMap(t -> expirarLote()
                        .doOnNext(n -> { if (n > 0) log.info("Expiradas {} reservas", n); })
                        .onErrorResume(e -> {
                            log.error("Tick de expiración falló: {}", e.toString());
                            return Mono.empty();
                        }))
                .retry()
                .subscribe();
        log.info("Job de expiración iniciado cada {}", props.expiryInterval());
    }

    public Mono<Long> expirarLote() {
        return ordenes.findByEstadoAndExpiraEnBefore(EstadoOrden.RESERVADA, Instant.now())
                .concatMap(o -> items.findByOrdenId(o.getId())
                        .collectList()
                        .flatMap(lista -> saga.liberarTodo(lista, o.getId()))
                        .then(Mono.defer(() -> {
                            o.setEstado(EstadoOrden.EXPIRADA);
                            o.setExpiraEn(null);
                            return ordenes.save(o);
                        }))
                        .doOnNext(saved -> bus.publicar(
                                EventoOrden.de(saved.getId(), EstadoOrden.EXPIRADA, "Reserva expirada")))
                        .onErrorResume(e -> {
                            log.error("No se pudo expirar orden {}: {}", o.getId(), e.toString());
                            return Mono.empty();
                        }))
                .count();
    }

    @PreDestroy
    public void detener() {
        if (suscripcion != null && !suscripcion.isDisposed()) {
            suscripcion.dispose();
            log.info("Job de expiración detenido");
        }
    }
}
```

---

## controller/

### 2.39 `src/main/java/com/example/demo/controller/OrdenCompraController.java`

```java
package com.example.demo.controller;

import com.example.demo.dto.CrearOrdenRequest;
import com.example.demo.dto.EventoOrden;
import com.example.demo.model.OrdenCompra;
import com.example.demo.service.OrdenCompraService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/orders")
public class OrdenCompraController {

    private final OrdenCompraService service;

    public OrdenCompraController(OrdenCompraService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<OrdenCompra>> crear(
            @RequestBody CrearOrdenRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        return service.crear(request, idempotencyKey)
                .map(o -> ResponseEntity.status(HttpStatus.CREATED).body(o));
    }

    @GetMapping("/{id}")
    public Mono<OrdenCompra> obtener(@PathVariable Long id) {
        return service.obtener(id);
    }

    @PostMapping("/{id}/confirm")
    public Mono<OrdenCompra> confirmar(@PathVariable Long id) {
        return service.confirmar(id);
    }

    @GetMapping(value = "/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<EventoOrden>> eventos(@PathVariable Long id) {
        return service.eventos(id)
                .map(e -> ServerSentEvent.builder(e)
                        .event(e.estado() == null ? "heartbeat" : "order")
                        .build());
    }
}
```

### 2.40 `src/main/java/com/example/demo/controller/TableroController.java`

```java
package com.example.demo.controller;

import com.example.demo.dto.EventoTablero;
import com.example.demo.service.TableroService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/ops")
public class TableroController {

    private final TableroService tablero;

    public TableroController(TableroService tablero) {
        this.tablero = tablero;
    }

    @GetMapping(value = "/dashboard", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<EventoTablero>> dashboard() {
        return tablero.stream()
                .map(e -> ServerSentEvent.builder(e).event(e.tipo()).build());
    }
}
```

### 2.41 `src/main/java/com/example/demo/controller/ReporteController.java`

```java
package com.example.demo.controller;

import com.example.demo.dto.TotalCategoria;
import com.example.demo.service.ReporteService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/api/reports")
public class ReporteController {

    private final ReporteService service;

    public ReporteController(ReporteService service) {
        this.service = service;
    }

    @GetMapping(value = "/sales", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<List<TotalCategoria>> ventas() {
        return service.ventasPorCategoria();
    }

    @GetMapping(value = "/sales/stream", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<TotalCategoria> ventasStream() {
        return service.totalCorriendo();
    }
}
```

### 2.42 `src/main/java/com/example/demo/controller/CargaMasivaController.java`

Va en un controller aparte de `ProductoController` (que no se toca) pero comparte el prefijo `/api/products`.

```java
package com.example.demo.controller;

import com.example.demo.dto.ResultadoCarga;
import com.example.demo.model.Producto;
import com.example.demo.service.CargaMasivaService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/products")
public class CargaMasivaController {

    private final CargaMasivaService service;

    public CargaMasivaController(CargaMasivaService service) {
        this.service = service;
    }

    @PostMapping(value = "/bulk", consumes = MediaType.APPLICATION_NDJSON_VALUE)
    public Mono<ResultadoCarga> bulk(@RequestBody Flux<Producto> productos) {
        return service.cargar(productos);
    }
}
```

### 2.43 `src/main/java/com/example/demo/controller/SimuladorExternoController.java`

```java
package com.example.demo.controller;

import com.example.demo.dto.CotizacionPrecio;
import com.example.demo.dto.SimuladorConfig;
import com.example.demo.repository.ProductReactiveRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Simula los tres servicios externos dentro de la misma app (puerto 8081). */
@RestController
@RequestMapping("/external")
public class SimuladorExternoController {

    /** Perillas para provocar fallos en la demo. Bean aparte para inyectarlo en los tests. */
    @Component
    public static class Perillas {
        public final AtomicInteger pricingFailures = new AtomicInteger(0);
        public final AtomicLong fraudDelayMs = new AtomicLong(50);
        public final AtomicInteger forcedRiskScore = new AtomicInteger(-1);

        public SimuladorConfig vista() {
            return new SimuladorConfig(pricingFailures.get(), fraudDelayMs.get(), forcedRiskScore.get());
        }

        public void aplicar(SimuladorConfig c) {
            if (c.pricingFailures() != null) pricingFailures.set(c.pricingFailures());
            if (c.fraudDelayMs() != null) fraudDelayMs.set(c.fraudDelayMs());
            if (c.forcedRiskScore() != null) forcedRiskScore.set(c.forcedRiskScore());
        }

        public void reset() {
            pricingFailures.set(0);
            fraudDelayMs.set(50);
            forcedRiskScore.set(-1);
        }
    }

    private final ProductReactiveRepository productos;
    private final Perillas perillas;

    public SimuladorExternoController(ProductReactiveRepository productos, Perillas perillas) {
        this.productos = productos;
        this.perillas = perillas;
    }

    @GetMapping("/pricing/{id}")
    public Mono<CotizacionPrecio> pricing(@PathVariable Long id) {
        int restantes = perillas.pricingFailures.getAndUpdate(n -> n > 0 ? n - 1 : 0);
        if (restantes > 0) {
            return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "pricing caído"));
        }
        return productos.findById(id)
                .map(p -> new CotizacionPrecio(id, Math.round(p.getPrice() * 1.10 * 100.0) / 100.0, "DYNAMIC"))
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "sin precio")));
    }

    @GetMapping("/tax/{region}")
    public Mono<Double> tax(@PathVariable String region) {
        double rate = "CO".equalsIgnoreCase(region) ? 0.19 : 0.16;
        return Mono.just(rate).delayElement(Duration.ofMillis(300));
    }

    @PostMapping("/fraud")
    public Mono<Integer> fraud(@RequestBody Map<String, Object> body) {
        int forzado = perillas.forcedRiskScore.get();
        String cliente = String.valueOf(body.getOrDefault("clienteId", ""));
        int score = forzado >= 0 ? forzado : Math.abs(cliente.hashCode()) % 60;
        return Mono.just(score).delayElement(Duration.ofMillis(perillas.fraudDelayMs.get()));
    }

    @GetMapping("/simulator")
    public SimuladorConfig actual() {
        return perillas.vista();
    }

    @PutMapping("/simulator")
    public SimuladorConfig configurar(@RequestBody SimuladorConfig config) {
        perillas.aplicar(config);
        return perillas.vista();
    }

    @DeleteMapping("/simulator")
    public SimuladorConfig reset() {
        perillas.reset();
        return perillas.vista();
    }
}
```

---

## PARTE 3 · Tests

### 3.1 `src/test/java/com/example/demo/common/ReactiveSupportTest.java`

```java
package com.example.demo.common;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ReactiveSupportTest {

    @Test
    void backoff_reintentaErroresTransitoriosYLuegoEmite() {
        AtomicInteger calls = new AtomicInteger();

        StepVerifier.withVirtualTime(() -> Mono.defer(() -> calls.incrementAndGet() < 3
                                ? Mono.<String>error(new TransientException("boom"))
                                : Mono.just("ok"))
                        .retryWhen(ReactiveSupport.backoff(3, Duration.ofMillis(200))))
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(5))
                .expectNext("ok")
                .verifyComplete();

        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void backoff_noReintentaErroresNoTransitorios() {
        AtomicInteger calls = new AtomicInteger();

        StepVerifier.withVirtualTime(() -> Mono.defer(() -> {
                            calls.incrementAndGet();
                            return Mono.<String>error(new IllegalArgumentException("fatal"));
                        })
                        .retryWhen(ReactiveSupport.backoff(3, Duration.ofMillis(200))))
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(5))
                .expectError(IllegalArgumentException.class)
                .verify();

        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void timeout_degradaAValorPorDefecto() {
        StepVerifier.withVirtualTime(() -> Mono.<Integer>never()
                        .timeout(Duration.ofMillis(800))
                        .onErrorReturn(TimeoutException.class, 50))
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(50)
                .verifyComplete();
    }
}
```

### 3.2 `src/test/java/com/example/demo/service/EventBusTest.java`

```java
package com.example.demo.service;

import com.example.demo.dto.EventoOrden;
import com.example.demo.model.EstadoOrden;
import com.example.demo.model.EventoInventario;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class EventBusTest {

    @Test
    void eventosOrden_esHotYReproduceHistoriaReciente() {
        EventBus bus = new EventBus();
        bus.publicar(EventoOrden.de(1L, EstadoOrden.RESERVADA, "antes de suscribir"));

        StepVerifier.create(bus.eventosOrden().take(2))
                .expectNextMatches(e -> e.mensaje().equals("antes de suscribir"))
                .then(() -> bus.publicar(EventoOrden.de(1L, EstadoOrden.CONFIRMADA, "después")))
                .expectNextMatches(e -> e.estado() == EstadoOrden.CONFIRMADA)
                .verifyComplete();
    }

    @Test
    void eventosInventario_descartaSiNadieEscucha() {
        EventBus bus = new EventBus();
        bus.publicar(EventoInventario.de("RESERVADO", 1L, -1, 1L)); // nadie suscrito → se descarta sin error

        StepVerifier.create(bus.eventosInventario().take(1))
                .then(() -> bus.publicar(EventoInventario.de("LIBERADO", 1L, 1, 1L)))
                .expectNextMatches(e -> e.getTipo().equals("LIBERADO"))
                .verifyComplete();
    }
}
```

### 3.3 `src/test/java/com/example/demo/service/ReservaSagaTest.java`

Sin base de datos: el `InventarioPort` se reemplaza por un fake en memoria.

```java
package com.example.demo.service;

import com.example.demo.common.DomainExceptions.ProductoNoExisteException;
import com.example.demo.common.DomainExceptions.StockInsuficienteException;
import com.example.demo.model.ItemOrden;
import com.example.demo.model.Producto;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class ReservaSagaTest {

    /** Inventario en memoria. Permite inyectar un Mono personalizado por producto (para TestPublisher). */
    static class FakeInventario implements InventarioPort {
        final Map<Long, Producto> productos = new ConcurrentHashMap<>();
        final List<String> log = new CopyOnWriteArrayList<>();
        Function<Long, Mono<Producto>> override = id -> null;

        FakeInventario con(long id, int stock, double price, String category) {
            Producto p = new Producto();
            p.setId(id); p.setStock(stock); p.setPrice(price); p.setCategory(category); p.setName("P" + id);
            productos.put(id, p);
            return this;
        }

        @Override public Mono<Producto> reservar(Long id, int cantidad, Long ordenId) {
            Mono<Producto> custom = override.apply(id);
            if (custom != null) return custom;
            return Mono.defer(() -> {
                Producto p = productos.get(id);
                if (p == null) return Mono.error(new ProductoNoExisteException(id));
                if (p.getStock() < cantidad) return Mono.error(new StockInsuficienteException(id, cantidad));
                p.setStock(p.getStock() - cantidad);
                log.add("reservar:" + id);
                return Mono.just(p);
            });
        }

        @Override public Mono<Void> liberar(Long id, int cantidad, Long ordenId) {
            return Mono.fromRunnable(() -> {
                Producto p = productos.get(id);
                p.setStock(p.getStock() + cantidad);
                log.add("liberar:" + id);
            });
        }

        @Override public Mono<Void> vender(Long id, int cantidad, Long ordenId) { return Mono.empty(); }
        @Override public Flux<Producto> stockBajo(int threshold) { return Flux.empty(); }
    }

    @Test
    void reservaTodo_yEnriqueceConCategoriaYPrecio() {
        FakeInventario inv = new FakeInventario().con(1, 10, 100.0, "A").con(2, 5, 20.0, "B");
        ReservaSaga saga = new ReservaSaga(inv);

        List<ItemOrden> items = List.of(
                new ItemOrden(7L, 1L, null, 2, null),
                new ItemOrden(7L, 2L, null, 1, null));

        StepVerifier.create(saga.reservarTodo(items, 7L))
                .assertNext(result -> {
                    assertThat(result).hasSize(2);
                    assertThat(result.get(0).getCategoria()).isEqualTo("A");
                    assertThat(result.get(0).getPrecioUnitario()).isEqualTo(100.0);
                    assertThat(result.get(0).getOrdenId()).isEqualTo(7L);
                })
                .verifyComplete();

        assertThat(inv.productos.get(1L).getStock()).isEqualTo(8);
        assertThat(inv.log).containsExactly("reservar:1", "reservar:2");
    }

    @Test
    void fallaSegundoItem_compensaElPrimero() {
        FakeInventario inv = new FakeInventario().con(1, 10, 100.0, "A").con(2, 0, 20.0, "B");
        ReservaSaga saga = new ReservaSaga(inv);

        List<ItemOrden> items = List.of(
                new ItemOrden(7L, 1L, null, 2, null),
                new ItemOrden(7L, 2L, null, 1, null));

        StepVerifier.create(saga.reservarTodo(items, 7L))
                .expectError(StockInsuficienteException.class)
                .verify();

        assertThat(inv.productos.get(1L).getStock()).isEqualTo(10);
        assertThat(inv.log).containsExactly("reservar:1", "liberar:1");
    }

    @Test
    void conTestPublisher_errorTardioTambienCompensa() {
        FakeInventario inv = new FakeInventario().con(1, 10, 100.0, "A").con(2, 5, 20.0, "B");
        TestPublisher<Producto> lento = TestPublisher.create();
        inv.override = id -> id == 2L ? lento.mono() : null;
        ReservaSaga saga = new ReservaSaga(inv);

        List<ItemOrden> items = List.of(
                new ItemOrden(7L, 1L, null, 3, null),
                new ItemOrden(7L, 2L, null, 1, null));

        StepVerifier.create(saga.reservarTodo(items, 7L))
                .then(() -> {
                    assertThat(inv.log).containsExactly("reservar:1"); // el 1 ya reservó, el 2 sigue pendiente
                    lento.error(new StockInsuficienteException(2L, 1));
                })
                .expectError(StockInsuficienteException.class)
                .verify();

        assertThat(inv.productos.get(1L).getStock()).isEqualTo(10);
        assertThat(inv.log).containsExactly("reservar:1", "liberar:1");
    }
}
```

### 3.4 `src/test/java/com/example/demo/service/ReporteServiceTest.java`

```java
package com.example.demo.service;

import com.example.demo.dto.TotalCategoria;
import com.example.demo.model.ItemOrden;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class ReporteServiceTest {

    private final ReporteService service = new ReporteService(null);

    @Test
    void agrupaPorCategoriaYOrdena() {
        Flux<ItemOrden> items = Flux.just(
                new ItemOrden(1L, 1L, "B", 2, 10.0),
                new ItemOrden(1L, 2L, "A", 1, 5.0),
                new ItemOrden(2L, 3L, "B", 1, 10.0),
                new ItemOrden(2L, 4L, null, 3, 1.0));

        StepVerifier.create(service.agregar(items))
                .assertNext(list -> {
                    assertThat(list).extracting(TotalCategoria::categoria)
                            .containsExactly("A", "B", "SIN_CATEGORIA");
                    assertThat(list.get(1).unidades()).isEqualTo(3);
                    assertThat(list.get(1).monto()).isEqualTo(30.0);
                })
                .verifyComplete();
    }

    @Test
    void fluxVacio_devuelveListaVacia() {
        StepVerifier.create(service.agregar(Flux.empty()))
                .assertNext(list -> assertThat(list).isEmpty())
                .verifyComplete();
    }
}
```

### 3.5 `src/test/java/com/example/demo/controller/OrdenFlowIntegrationTest.java`

Requiere Postgres corriendo (`docker compose up -d`). Usa puerto fijo **18081** para que los servicios simulados
se llamen a sí mismos. El seed usa el upsert de `CargaMasivaService` porque `repository.save()` con `id` fijo
en R2DBC sería un `UPDATE` sin fila que actualizar.

```java
package com.example.demo.controller;

import com.example.demo.dto.EventoTablero;
import com.example.demo.model.OrdenCompra;
import com.example.demo.model.Producto;
import com.example.demo.repository.ItemOrdenRepository;
import com.example.demo.repository.OrdenCompraRepository;
import com.example.demo.repository.ProductReactiveRepository;
import com.example.demo.service.CargaMasivaService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.http.MediaType;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT,
        properties = {
                "server.port=18081",
                "app.external.base-url=http://localhost:18081",
                "app.expiry-interval=PT1H"
        })
@AutoConfigureWebTestClient(timeout = "30000")
class OrdenFlowIntegrationTest {

    @Autowired WebTestClient client;
    @Autowired ProductReactiveRepository productos;
    @Autowired OrdenCompraRepository ordenes;
    @Autowired ItemOrdenRepository items;
    @Autowired CargaMasivaService carga;
    @Autowired DatabaseClient db;
    @Autowired SimuladorExternoController.Perillas perillas;

    @BeforeEach
    void sembrar() {
        perillas.reset();
        limpiar();
        carga.cargar(Flux.just(
                        producto(1L, "Teclado", 100.0, 10, "PERIFERICOS"),
                        producto(2L, "Mouse", 50.0, 1, "PERIFERICOS"),
                        producto(3L, "Monitor", 900.0, 0, "PANTALLAS")))
                .block(Duration.ofSeconds(10));
    }

    @AfterEach
    void limpiar() {
        // orden_item cae por ON DELETE CASCADE; productos se truncan aparte
        db.sql("DELETE FROM orden_compra").fetch().rowsUpdated()
                .then(db.sql("DELETE FROM evento_inventario").fetch().rowsUpdated())
                .then(db.sql("DELETE FROM productos").fetch().rowsUpdated())
                .block(Duration.ofSeconds(10));
        perillas.reset();
    }

    @Test
    @DisplayName("E1 orden feliz: 201, RESERVADA, stock descontado, total con impuesto")
    void ordenFeliz() {
        client.post().uri("/api/orders")
                .header("X-Correlation-Id", "test-e1")
                .bodyValue(request(1L, "CO", 1L, 2))
                .exchange()
                .expectStatus().isCreated()
                .expectHeader().valueEquals("X-Correlation-Id", "test-e1")
                .expectBody()
                .jsonPath("$.estado").isEqualTo("RESERVADA")
                .jsonPath("$.items[0].categoria").isEqualTo("PERIFERICOS")
                .jsonPath("$.subtotal").isEqualTo(220.0)   // 2 × 110 (precio dinámico = catálogo × 1.10)
                .jsonPath("$.impuesto").isEqualTo(41.8)
                .jsonPath("$.total").isEqualTo(261.8);

        assertThat(stock(1L)).isEqualTo(8);
        assertThat(reserved(1L)).isEqualTo(2);
    }

    @Test
    @DisplayName("E2 producto inexistente: 404 y la orden queda COMPENSADA")
    void productoInexistente() {
        client.post().uri("/api/orders")
                .bodyValue(request(1L, "CO", 9999L, 1))
                .exchange()
                .expectStatus().isNotFound()
                .expectBody().jsonPath("$.error").isEqualTo("ProductoNoExisteException");

        assertThat(ordenes.findAll().collectList().block(Duration.ofSeconds(5)))
                .allMatch(o -> o.getEstado().esTerminal());
    }

    @Test
    @DisplayName("E3 stock insuficiente en el segundo ítem: 409 y compensación del primero")
    void compensacion() {
        client.post().uri("/api/orders")
                .bodyValue(Map.of("clienteId", 1, "region", "CO", "items", List.of(
                        Map.of("productoId", 1, "cantidad", 2),
                        Map.of("productoId", 3, "cantidad", 1))))
                .exchange()
                .expectStatus().isEqualTo(409);

        assertThat(stock(1L)).isEqualTo(10);
        assertThat(reserved(1L)).isZero();
        assertThat(stock(3L)).isZero();
    }

    @Test
    @DisplayName("E5 precios caídos: fallback al catálogo y 201")
    void fallbackPrecioCatalogo() {
        perillas.pricingFailures.set(50);

        client.post().uri("/api/orders")
                .bodyValue(request(1L, "MX", 1L, 1))
                .exchange()
                .expectStatus().isCreated()
                .expectBody()
                .jsonPath("$.subtotal").isEqualTo(100.0)   // precio de catálogo, no 110
                .jsonPath("$.impuesto").isEqualTo(16.0);
    }

    @Test
    @DisplayName("E7 riesgo alto: 422, reserva liberada, orden RECHAZADA")
    void riesgoAlto() {
        perillas.forcedRiskScore.set(95);

        client.post().uri("/api/orders")
                .bodyValue(request(1L, "CO", 1L, 2))
                .exchange()
                .expectStatus().isEqualTo(422);

        assertThat(stock(1L)).isEqualTo(10);
        assertThat(ordenes.findAll().collectList().block(Duration.ofSeconds(5)))
                .anyMatch(o -> o.getEstado().name().equals("RECHAZADA"));
    }

    @Test
    @DisplayName("E8 idempotencia: misma clave devuelve la misma orden y no descuenta dos veces")
    void idempotencia() {
        Long id1 = crear("K1");
        Long id2 = crear("K1");

        assertThat(id1).isEqualTo(id2);
        assertThat(stock(1L)).isEqualTo(8);
        assertThat(items.findByOrdenId(id1).count().block(Duration.ofSeconds(5))).isEqualTo(1);
    }

    @Test
    @DisplayName("Confirmar pasa a CONFIRMADA y el stream SSE de la orden cierra en estado terminal")
    void confirmarYStreamCierra() {
        Long id = crear(null);

        client.post().uri("/api/orders/{id}/confirm", id)
                .exchange().expectStatus().isOk()
                .expectBody().jsonPath("$.estado").isEqualTo("CONFIRMADA");

        assertThat(reserved(1L)).isZero();

        // La orden ya es terminal → el stream emite el estado actual y completa por takeUntil
        List<String> eventos = client.get().uri("/api/orders/{id}/events", id)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange().expectStatus().isOk()
                .returnResult(String.class).getResponseBody()
                .collectList().block(Duration.ofSeconds(10));

        assertThat(eventos).isNotEmpty();
        assertThat(String.join("", eventos)).contains("CONFIRMADA");
    }

    @Test
    @DisplayName("El reporte agrega solo órdenes CONFIRMADAS")
    void reporteSoloConfirmadas() {
        Long id = crear(null);
        client.post().uri("/api/orders/{id}/confirm", id).exchange().expectStatus().isOk();

        client.get().uri("/api/reports/sales")
                .exchange().expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].categoria").isEqualTo("PERIFERICOS")
                .jsonPath("$[0].unidades").isEqualTo(1);
    }

    @Test
    @DisplayName("Dashboard SSE recibe el evento de una orden nueva")
    void dashboardRecibeEventos() {
        var dashboard = client.get().uri("/api/ops/dashboard")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange().expectStatus().isOk()
                .returnResult(EventoTablero.class).getResponseBody()
                .filter(e -> "order".equals(e.tipo()))
                .next()
                .timeout(Duration.ofSeconds(15))
                .toFuture();

        client.post().uri("/api/orders")
                .bodyValue(request(1L, "CO", 1L, 1))
                .exchange().expectStatus().isCreated();

        assertThat(dashboard.join().tipo()).isEqualTo("order");
    }

    // ---- helpers ----

    private Long crear(String idempotencyKey) {
        var spec = client.post().uri("/api/orders");
        if (idempotencyKey != null) spec = spec.header("Idempotency-Key", idempotencyKey);
        return spec.bodyValue(request(1L, "CO", 1L, 1))
                .exchange().expectStatus().isCreated()
                .returnResult(OrdenCompra.class).getResponseBody()
                .blockFirst(Duration.ofSeconds(15))
                .getId();
    }

    private static Producto producto(Long id, String name, double price, int stock, String category) {
        Producto p = new Producto();
        p.setId(id); p.setName(name); p.setPrice(price); p.setStock(stock); p.setCategory(category);
        return p;
    }

    private static Map<String, Object> request(Long clienteId, String region, Long productoId, int cantidad) {
        return Map.of("clienteId", clienteId, "region", region,
                "items", List.of(Map.of("productoId", productoId, "cantidad", cantidad)));
    }

    private int stock(Long id) {
        return productos.findById(id).block(Duration.ofSeconds(5)).getStock();
    }

    private int reserved(Long id) {
        return productos.findById(id).block(Duration.ofSeconds(5)).getReserved();
    }
}
```

> `E8 idempotencia` asume que la orden se crea con `Idempotency-Key`. Si dos peticiones concurrentes usan la misma
> clave, el índice único `ux_orden_compra_idem` hace fallar la segunda inserción con
> `DataIntegrityViolationException` en lugar de devolver la orden existente: esa carrera se resuelve capturando
> el error y releyendo por clave. Queda como ejercicio.

---

## PARTE 4 · Ejecutar y probar a mano

```bash
docker compose up -d          # postgres:15 en 5432
./gradlew clean test
./gradlew bootRun             # app en 8081
```

Sembrar productos (NDJSON, un objeto por línea):

```bash
curl -X POST localhost:8081/api/products/bulk -H 'Content-Type: application/x-ndjson' --data-binary $'{"id":1,"name":"Teclado","price":100,"stock":10,"category":"PERIFERICOS"}\n{"id":2,"name":"Mouse","price":50,"stock":3,"category":"PERIFERICOS"}\n{"id":3,"name":"Monitor","price":900,"stock":0,"category":"PANTALLAS"}\n'
```

Crear un cliente (las órdenes referencian `clientes.id`):

```bash
curl -X POST localhost:8081/api/clientes -H 'Content-Type: application/json' \
  -d '{"nombre":"Ana","email":"ana@mail.com"}'
```

Abrir el tablero en otra terminal (queda escuchando):

```bash
curl -N localhost:8081/api/ops/dashboard
```

Orden feliz:

```bash
curl -i -X POST localhost:8081/api/orders -H 'Content-Type: application/json' \
  -H 'X-Correlation-Id: demo-1' -H 'Idempotency-Key: K1' \
  -d '{"clienteId":1,"region":"CO","items":[{"productoId":1,"cantidad":2},{"productoId":2,"cantidad":1}]}'
```

Repetir el mismo comando: misma orden (idempotencia). Stock insuficiente (compensa el ítem 1):

```bash
curl -i -X POST localhost:8081/api/orders -H 'Content-Type: application/json' \
  -d '{"clienteId":1,"region":"CO","items":[{"productoId":1,"cantidad":1},{"productoId":3,"cantidad":1}]}'
```

Provocar fallos externos:

```bash
# precios caen 2 veces y luego responden → ver reintentos en el log
curl -X PUT localhost:8081/external/simulator -H 'Content-Type: application/json' -d '{"pricingFailures":2}'
# antifraude tarda 3 s → timeout 800 ms y score por defecto
curl -X PUT localhost:8081/external/simulator -H 'Content-Type: application/json' -d '{"fraudDelayMs":3000}'
# riesgo alto → 422 y liberación
curl -X PUT localhost:8081/external/simulator -H 'Content-Type: application/json' -d '{"forcedRiskScore":95}'
# volver a normal
curl -X DELETE localhost:8081/external/simulator
```

Stream de una orden, confirmar, reporte:

```bash
curl -N localhost:8081/api/orders/<ID>/events
curl -X POST localhost:8081/api/orders/<ID>/confirm
curl localhost:8081/api/reports/sales
curl -N localhost:8081/api/reports/sales/stream
```

Ver el efecto en la base:

```bash
docker exec -it postgres_r2dbc psql -U postgres -d testdb \
  -c 'SELECT id, estado, subtotal, impuesto, total, risk_score FROM orden_compra ORDER BY id DESC LIMIT 5;' \
  -c 'SELECT id, name, stock, reserved FROM productos ORDER BY id;' \
  -c 'SELECT tipo, producto_id, delta, orden_id FROM evento_inventario ORDER BY id DESC LIMIT 10;'
```

Expiración: bajar `app.reservation-ttl` a `20s` y `app.expiry-interval` a `5s` en `application.yml`, crear una orden
sin confirmar y ver en el dashboard el evento `EXPIRADA` con el stock devuelto (`stock` sube, `reserved` baja).

---

## PARTE 5 · Notas de R2DBC que muerden

1. **`save()` con `@Id` no nulo = `UPDATE`.** Sin `Persistable#isNew` no hay insert. Para ids controlados desde
   afuera, usa upsert por SQL (bloque 2.37) o deja que Postgres genere el id.
2. **Sin relaciones.** No hay `@OneToMany` ni carga perezosa: la lista de ítems se consulta aparte y se ensambla
   en el service (`conItems`). Un `@Transient` en la entidad evita que R2DBC intente mapear la columna.
3. **Transacciones sí, pero cortas.** `TransactionalOperator` funciona bien alrededor de escrituras. No envuelvas
   las llamadas `WebClient` en la transacción: mantendría filas bloqueadas mientras un tercero responde. Por eso
   la reserva es una saga con compensación y no una transacción larga.
4. **`@Transactional` sobre un método que devuelve `Mono`** funciona; sobre un método que devuelve un `Flux` que se
   suscribe fuera del scope, no. Con `.as(tx::transactional)` el alcance es explícito.
5. **`Instant` ↔ `TIMESTAMPTZ`** lo mapea `r2dbc-postgresql`. Con `TIMESTAMP` (sin zona) hay que usar
   `LocalDateTime` o registrar un converter.
6. **`DECIMAL` llega como `BigDecimal`.** Al mapear a mano (`DatabaseClient`) hay que convertir; los repositorios
   lo hacen solos contra `Double`.
7. **Enums** se guardan como `varchar` por conversión estándar. Para tipos enum nativos de Postgres hace falta
   `EnumWriteSupport`.
8. **`spring.sql.init` usa JDBC**, no R2DBC: por eso `org.postgresql:postgresql` sigue en las dependencias
   (`runtimeOnly`) aunque el acceso de la app sea 100 % reactivo.
