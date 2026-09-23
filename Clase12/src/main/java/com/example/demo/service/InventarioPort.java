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
