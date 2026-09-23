package com.example.demo.service;

import com.example.demo.dto.CotizacionPrecio;
import reactor.core.publisher.Mono;

public interface ServiciosExternosPort {
    Mono<CotizacionPrecio> precio(Long productoId);
    Mono<Double> tasaImpuesto(String region);
    Mono<Integer> scoreRiesgo(String clienteId, double totalEstimado);
}
