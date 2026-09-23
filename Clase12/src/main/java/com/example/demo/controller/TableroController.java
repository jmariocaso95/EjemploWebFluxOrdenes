package com.example.demo.controller;

import com.example.demo.dto.EventoTablero;
import com.example.demo.service.TableroService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Tablero")
@RestController
@RequestMapping("/api/ops")
public class TableroController {

    private final TableroService tablero;

    public TableroController(TableroService tablero) {
        this.tablero = tablero;
    }

    @Operation(summary = "Stream SSE global de eventos",
            description = "Publisher caliente compartido entre todos los suscriptores, con heartbeat periódico.")
    @GetMapping(value = "/dashboard", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<EventoTablero>> dashboard() {
        return tablero.stream()
                .map(e -> ServerSentEvent.builder(e).event(e.tipo()).build());
    }
}
