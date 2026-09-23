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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Simulador externo")
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

    @Operation(summary = "Precio dinámico simulado (catálogo × 1.10)")
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

    @Operation(summary = "Tasa de impuesto por región (lenta, se cachea 10 min en el cliente)")
    @GetMapping("/tax/{region}")
    public Mono<Double> tax(@PathVariable String region) {
        double rate = "CO".equalsIgnoreCase(region) ? 0.19 : 0.16;
        return Mono.just(rate).delayElement(Duration.ofMillis(300));
    }

    @Operation(summary = "Score antifraude simulado (puede tardar más que el timeout)")
    @PostMapping("/fraud")
    public Mono<Integer> fraud(@RequestBody Map<String, Object> body) {
        int forzado = perillas.forcedRiskScore.get();
        String cliente = String.valueOf(body.getOrDefault("clienteId", ""));
        int score = forzado >= 0 ? forzado : Math.abs(cliente.hashCode()) % 60;
        return Mono.just(score).delayElement(Duration.ofMillis(perillas.fraudDelayMs.get()));
    }

    @Operation(summary = "Configuración actual del simulador")
    @GetMapping("/simulator")
    public SimuladorConfig actual() {
        return perillas.vista();
    }

    @Operation(summary = "Ajusta fallos de precio, latencia de antifraude y riesgo forzado")
    @PutMapping("/simulator")
    public SimuladorConfig configurar(@RequestBody SimuladorConfig config) {
        perillas.aplicar(config);
        return perillas.vista();
    }

    @Operation(summary = "Restaura el simulador a los valores por defecto")
    @DeleteMapping("/simulator")
    public SimuladorConfig reset() {
        perillas.reset();
        return perillas.vista();
    }
}
