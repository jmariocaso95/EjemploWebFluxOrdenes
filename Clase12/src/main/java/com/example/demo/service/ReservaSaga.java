package com.example.demo.service;

import com.example.demo.model.ItemOrden;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Reserva secuencial (concatMap) y compensación de lo ya reservado si algo falla.
 * Cada reserva es su propia transacción de fila: la consistencia entre productos la da esta saga,
 * no una transacción larga que mantendría filas bloqueadas mientras se llama a servicios externos.
 */
@Component
public class ReservaSaga {

    private static final Logger log = LoggerFactory.getLogger(ReservaSaga.class);

    private final InventarioPort inventario;

    public ReservaSaga(InventarioPort inventario) {
        this.inventario = inventario;
    }

    /** Devuelve los ítems enriquecidos con categoría y precio de catálogo. */
    public Mono<List<ItemOrden>> reservarTodo(List<ItemOrden> items, Long ordenId) {
        return Mono.defer(() -> {
            List<ItemOrden> hechos = new CopyOnWriteArrayList<>();
            return Flux.fromIterable(items)
                    .concatMap(i -> inventario.reservar(i.getProductoId(), i.getCantidad(), ordenId)
                            .map(p -> new ItemOrden(ordenId, i.getProductoId(), p.getCategory(),
                                    i.getCantidad(), p.getPrice())))
                    .doOnNext(hechos::add)
                    .collectList()
                    .onErrorResume(ex -> {
                        log.warn("Reserva fallida en orden {}: {}. Compensando {} ítems",
                                ordenId, ex.getMessage(), hechos.size());
                        return liberarTodo(hechos, ordenId).then(Mono.error(ex));
                    });
        });
    }

    public Mono<Void> liberarTodo(List<ItemOrden> items, Long ordenId) {
        if (items == null || items.isEmpty()) return Mono.empty();
        return Flux.fromIterable(items)
                .concatMap(i -> inventario.liberar(i.getProductoId(), i.getCantidad(), ordenId)
                        .onErrorResume(e -> {
                            log.error("No se pudo liberar producto {} de orden {}: {}",
                                    i.getProductoId(), ordenId, e.toString());
                            return Mono.empty();
                        }))
                .then();
    }
}
