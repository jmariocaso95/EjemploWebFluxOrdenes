package com.example.demo.service;

import com.example.demo.common.AppProperties;
import com.example.demo.dto.EventoOrden;
import com.example.demo.model.EstadoOrden;
import com.example.demo.repository.ItemOrdenRepository;
import com.example.demo.repository.OrdenCompraRepository;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Flux.interval como reloj. onBackpressureDrop evita solapar ticks si uno tarda.
 * Un error de una orden no mata el job; un error del tick tampoco (onErrorResume + retry).
 */
@Component
public class ExpiracionReservasJob {

    private static final Logger log = LoggerFactory.getLogger(ExpiracionReservasJob.class);

    private final OrdenCompraRepository ordenes;
    private final ItemOrdenRepository items;
    private final ReservaSaga saga;
    private final EventBus bus;
    private final AppProperties props;
    private Disposable suscripcion;

    public ExpiracionReservasJob(OrdenCompraRepository ordenes, ItemOrdenRepository items,
                                 ReservaSaga saga, EventBus bus, AppProperties props) {
        this.ordenes = ordenes;
        this.items = items;
        this.saga = saga;
        this.bus = bus;
        this.props = props;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void iniciar() {
        suscripcion = Flux.interval(props.expiryInterval())
                .onBackpressureDrop(t -> log.warn("Tick {} descartado: el anterior aún corre", t))
                .concatMap(t -> expirarLote()
                        .doOnNext(n -> { if (n > 0) log.info("Expiradas {} reservas", n); })
                        .onErrorResume(e -> {
                            log.error("Tick de expiración falló: {}", e.toString());
                            return Mono.empty();
                        }))
                .retry()
                .subscribe();
        log.info("Job de expiración iniciado cada {}", props.expiryInterval());
    }

    public Mono<Long> expirarLote() {
        return ordenes.findByEstadoAndExpiraEnBefore(EstadoOrden.RESERVADA, Instant.now())
                .concatMap(o -> items.findByOrdenId(o.getId())
                        .collectList()
                        .flatMap(lista -> saga.liberarTodo(lista, o.getId()))
                        .then(Mono.defer(() -> {
                            o.setEstado(EstadoOrden.EXPIRADA);
                            o.setExpiraEn(null);
                            return ordenes.save(o);
                        }))
                        .doOnNext(saved -> bus.publicar(
                                EventoOrden.de(saved.getId(), EstadoOrden.EXPIRADA, "Reserva expirada")))
                        .onErrorResume(e -> {
                            log.error("No se pudo expirar orden {}: {}", o.getId(), e.toString());
                            return Mono.empty();
                        }))
                .count();
    }

    @PreDestroy
    public void detener() {
        if (suscripcion != null && !suscripcion.isDisposed()) {
            suscripcion.dispose();
            log.info("Job de expiración detenido");
        }
    }
}
