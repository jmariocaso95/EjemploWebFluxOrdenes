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
        assertThat(stock(1L)).isEqualTo(9);   // una sola unidad descontada pese a las dos llamadas
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
