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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Productos")
@RestController
@RequestMapping("/api/products")
public class CargaMasivaController {

    private final CargaMasivaService service;

    public CargaMasivaController(CargaMasivaService service) {
        this.service = service;
    }

    @Operation(summary = "Carga masiva de productos (NDJSON)",
            description = "Un JSON por línea. Se procesa en lotes de 500 con INSERT ... ON CONFLICT DO UPDATE.")
    @PostMapping(value = "/bulk", consumes = MediaType.APPLICATION_NDJSON_VALUE)
    public Mono<ResultadoCarga> bulk(@RequestBody Flux<Producto> productos) {
        return service.cargar(productos);
    }
}
