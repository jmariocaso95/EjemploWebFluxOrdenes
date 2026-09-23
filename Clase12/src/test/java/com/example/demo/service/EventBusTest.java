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
