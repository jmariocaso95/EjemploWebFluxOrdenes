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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Reportes")
@RestController
@RequestMapping("/api/reports")
public class ReporteController {

    private final ReporteService service;

    public ReporteController(ReporteService service) {
        this.service = service;
    }

    @Operation(summary = "Ventas por categoría",
            description = "Agrega los ítems de las órdenes CONFIRMADAS respetando backpressure (limitRate).")
    @GetMapping(value = "/sales", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<List<TotalCategoria>> ventas() {
        return service.ventasPorCategoria();
    }

    @Operation(summary = "Total acumulado en vivo (NDJSON)",
            description = "Un objeto por línea con el acumulado (scan). Respuesta infinita: cerrar el cliente para terminar.")
    @GetMapping(value = "/sales/stream", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<TotalCategoria> ventasStream() {
        return service.totalCorriendo();
    }
}
