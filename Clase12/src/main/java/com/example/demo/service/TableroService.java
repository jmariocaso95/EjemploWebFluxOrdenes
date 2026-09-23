package com.example.demo.service;

import com.example.demo.common.AppProperties;
import com.example.demo.dto.EventoTablero;
import com.example.demo.model.Producto;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;

/**
 * Un solo Flux compartido entre todos los operadores (publish().refCount).
 * Se enciende con el primer suscriptor y se apaga 5 s después del último.
 */
@Service
public class TableroService {

    private final Flux<EventoTablero> compartido;

    public TableroService(EventBus bus, InventarioPort inventario, AppProperties props) {
        Flux<EventoTablero> deOrdenes = bus.eventosOrden()
                .map(e -> EventoTablero.de("order", e));

        Flux<EventoTablero> deInventario = bus.eventosInventario()
                .map(e -> EventoTablero.de("inventory", e));

        Flux<EventoTablero> stockBajo = Flux.interval(Duration.ZERO, Duration.ofSeconds(10))
                .onBackpressureDrop()
                .concatMap(t -> inventario.stockBajo(props.lowStockThreshold()).map(Producto::getId).collectList())
                .distinctUntilChanged()
                .filter(ids -> !ids.isEmpty())
                .map(ids -> EventoTablero.de("low-stock", ids));

        Flux<EventoTablero> porVentana = bus.eventosOrden()
                .window(Duration.ofSeconds(30))
                .flatMap(Flux::count)
                .map(c -> EventoTablero.de("orders-per-30s", c));

        this.compartido = Flux.merge(deOrdenes, deInventario, stockBajo, porVentana)
                .publish()
                .refCount(1, Duration.ofSeconds(5));
    }

    /** Cliente lento recibe el último evento, no una cola infinita. */
    public Flux<EventoTablero> stream() {
        return compartido.onBackpressureLatest();
    }
}
