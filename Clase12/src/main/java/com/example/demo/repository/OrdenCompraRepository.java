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
