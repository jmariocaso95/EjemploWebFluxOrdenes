package com.example.demo;

import com.example.demo.model.Producto;
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
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest
@AutoConfigureWebTestClient(timeout = "30000")
class ProductoControllerTest {

    private static final int CATALOGO = 8;

    @Autowired
    private WebTestClient client;

    @Autowired
    private ProductReactiveRepository repository;

    @Autowired
    private CargaMasivaService carga;

    @BeforeEach
    void sembrarCatalogo() {
        List<Producto> catalogo = IntStream.rangeClosed(1, CATALOGO)
                .mapToObj(i -> {
                    Producto p = new Producto();
                    p.setId((long) i);
                    p.setName("Producto " + i);
                    p.setPrice(i * 10.0);
                    p.setStock(i);
                    p.setCategory("DEMO");
                    return p;
                })
                .toList();

        // R2DBC interpreta un @Id no nulo como UPDATE: hay que sembrar con upsert
        repository.deleteAll().block(Duration.ofSeconds(10));
        carga.cargar(reactor.core.publisher.Flux.fromIterable(catalogo))
                .block(Duration.ofSeconds(10));
    }

    @AfterEach
    void limpiarCatalogo() {
        repository.deleteAll().block(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("GET /api/products devuelve el catalogo de la demo")
    void listado_devuelveElCatalogo() {
        List<Producto> productos = client.get().uri("/api/products")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(Producto.class)
                .returnResult()
                .getResponseBody();

        assertThat(productos).isNotNull().hasSize(CATALOGO);
    }

    @Test
    @DisplayName("GET /api/products/{id} inexistente devuelve 404")
    void porId_inexistente_devuelve404() {
        client.get().uri("/api/products/9999")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    @DisplayName("el /stream emite Server-Sent Events, no un JSON de una sola vez")
    void stream_emiteEventos() {
        client.get().uri("/api/products/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM);
    }

    /**
     * La prueba que demuestra la diferencia: se piden solo los 3 primeros elementos
     * y el flujo se corta sin esperar los 8. Con una List<Product> bloqueante
     * habría que materializar todo antes de responder.
     */
    @Test
    @DisplayName("el consumidor puede tomar 3 elementos y cortar el flujo")
    void stream_permiteCortarElFlujo() {
        List<Producto> primeros = client.get().uri("/api/products/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .returnResult(Producto.class)
                .getResponseBody()
                .take(3)
                .collectList()
                .block(Duration.ofSeconds(20));

        assertThat(primeros).hasSize(3);
    }
}
