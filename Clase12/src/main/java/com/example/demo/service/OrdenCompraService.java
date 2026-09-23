package com.example.demo.service;

import com.example.demo.common.AppProperties;
import com.example.demo.common.CorrelationWebFilter;
import com.example.demo.common.DomainExceptions.EstadoInvalidoException;
import com.example.demo.common.DomainExceptions.OrdenNoExisteException;
import com.example.demo.common.DomainExceptions.RiesgoAltoException;
import com.example.demo.common.DomainExceptions.ValidacionException;
import com.example.demo.common.ReactiveSupport;
import com.example.demo.dto.CrearOrdenRequest;
import com.example.demo.dto.EventoOrden;
import com.example.demo.model.EstadoOrden;
import com.example.demo.model.ItemOrden;
import com.example.demo.model.OrdenCompra;
import com.example.demo.repository.ItemOrdenRepository;
import com.example.demo.repository.OrdenCompraRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class OrdenCompraService {

    private static final Logger log = LoggerFactory.getLogger(OrdenCompraService.class);

    private final OrdenCompraRepository ordenes;
    private final ItemOrdenRepository items;
    private final ReservaSaga saga;
    private final InventarioPort inventario;
    private final ServiciosExternosPort externos;
    private final EventBus bus;
    private final AppProperties props;
    private final TransactionalOperator tx;

    public OrdenCompraService(OrdenCompraRepository ordenes, ItemOrdenRepository items, ReservaSaga saga,
                              InventarioPort inventario, ServiciosExternosPort externos, EventBus bus,
                              AppProperties props, TransactionalOperator tx) {
        this.ordenes = ordenes;
        this.items = items;
        this.saga = saga;
        this.inventario = inventario;
        this.externos = externos;
        this.bus = bus;
        this.props = props;
        this.tx = tx;
    }

    // ---------- Crear ----------

    public Mono<OrdenCompra> crear(CrearOrdenRequest req, String idempotencyKey) {
        return validar(req)
                .then(buscarExistente(idempotencyKey))
                .switchIfEmpty(Mono.defer(() -> crearNueva(req, idempotencyKey)));
    }

    private Mono<Void> validar(CrearOrdenRequest req) {
        if (req == null || req.items() == null || req.items().isEmpty()) {
            return Mono.error(new ValidacionException("La orden debe tener al menos un ítem"));
        }
        if (req.clienteId() == null) {
            return Mono.error(new ValidacionException("clienteId es obligatorio"));
        }
        boolean cantidadMala = req.items().stream()
                .anyMatch(i -> i.productoId() == null || i.cantidad() == null || i.cantidad() <= 0);
        if (cantidadMala) {
            return Mono.error(new ValidacionException("Cada ítem requiere productoId y cantidad > 0"));
        }
        return Mono.empty();
    }

    private Mono<OrdenCompra> buscarExistente(String key) {
        if (key == null || key.isBlank()) return Mono.empty();
        return ordenes.findByIdempotencyKey(key)
                .doOnNext(o -> log.info("Idempotency-Key {} ya procesada → orden {}", key, o.getId()))
                .flatMap(this::conItems);
    }

    private Mono<OrdenCompra> crearNueva(CrearOrdenRequest req, String key) {
        AtomicReference<List<ItemOrden>> reservados = new AtomicReference<>(List.of());

        Mono<OrdenCompra> flujo = ordenes.save(OrdenCompra.nueva(req.clienteId(), req.region(), key))
                .flatMap(orden -> {
                    List<ItemOrden> pedidos = req.items().stream()
                            .map(i -> new ItemOrden(orden.getId(), i.productoId(), null, i.cantidad(), null))
                            .toList();

                    return ReactiveSupport.traced("reserva", saga.reservarTodo(pedidos, orden.getId()))
                            .doOnNext(reservados::set)
                            .doOnNext(reservadosOk -> {
                                orden.setItems(reservadosOk);
                                orden.setEstado(EstadoOrden.RESERVADA);
                                orden.setExpiraEn(Instant.now().plus(props.reservationTtl()));
                            })
                            .flatMap(reservadosOk -> ReactiveSupport.traced("precios+impuesto+fraude",
                                    tarificar(orden, reservadosOk)))
                            .flatMap(o -> o.getRiskScore() > props.riskThreshold()
                                    ? Mono.<OrdenCompra>error(new RiesgoAltoException(o.getRiskScore()))
                                    : Mono.just(o))
                            .flatMap(this::persistir)
                            .doOnNext(o -> bus.publicar(EventoOrden.de(o.getId(), o.getEstado(),
                                    "Stock reservado, total " + o.getTotal())))
                            .onErrorResume(ex -> compensar(orden, reservados.get(), ex));
                });

        return Mono.deferContextual(ctx -> {
            String cid = ctx.getOrDefault(CorrelationWebFilter.KEY, "n/a");
            return flujo
                    .doOnSubscribe(s -> log.info("[{}] Creando orden para cliente {}", cid, req.clienteId()))
                    .doOnError(e -> log.warn("[{}] Orden falló: {}", cid, e.toString()))
                    .doFinally(sig -> log.info("[{}] Flujo de creación terminó con señal {}", cid, sig));
        });
    }

    /** Tres llamadas externas en paralelo con Mono.zip; cálculo CPU en Schedulers.parallel(). */
    private Mono<OrdenCompra> tarificar(OrdenCompra orden, List<ItemOrden> reservados) {
        double estimado = reservados.stream().mapToDouble(ItemOrden::totalLinea).sum();

        Mono<List<ItemOrden>> conPrecio = Flux.fromIterable(reservados)
                .flatMap(i -> externos.precio(i.getProductoId())
                        .map(q -> i.conPrecio(q.precioUnitario()))
                        .onErrorResume(ex -> {
                            log.warn("Precio dinámico no disponible para {} ({}); fallback catálogo",
                                    i.getProductoId(), ex.getClass().getSimpleName());
                            return Mono.just(i);
                        }), 8)
                .collectList();

        Mono<Double> tasa = externos.tasaImpuesto(orden.getRegion());
        Mono<Integer> riesgo = externos.scoreRiesgo(String.valueOf(orden.getClienteId()), estimado);

        return Mono.zip(conPrecio, tasa, riesgo)
                .publishOn(Schedulers.parallel())
                .map(t -> {
                    List<ItemOrden> tarificados = t.getT1();
                    double subtotal = redondear(tarificados.stream().mapToDouble(ItemOrden::totalLinea).sum());
                    double impuesto = redondear(subtotal * t.getT2());
                    orden.setItems(tarificados);
                    orden.setSubtotal(subtotal);
                    orden.setImpuesto(impuesto);
                    orden.setTotal(redondear(subtotal + impuesto));
                    orden.setRiskScore(t.getT3());
                    return orden;
                });
    }

    /** Ítems + cabecera en una sola transacción: o queda todo o no queda nada. */
    private Mono<OrdenCompra> persistir(OrdenCompra orden) {
        List<ItemOrden> aGuardar = orden.getItems().stream()
                .map(i -> i.conOrden(orden.getId()))
                .toList();

        Mono<OrdenCompra> escritura = items.deleteAll(items.findByOrdenId(orden.getId()))
                .thenMany(items.saveAll(aGuardar))
                .collectList()
                .flatMap(guardados -> ordenes.save(orden).doOnNext(o -> o.setItems(guardados)));

        return escritura.as(tx::transactional);
    }

    /** Si ya había reserva, la devuelve, persiste el estado final y re-emite el error original. */
    private Mono<OrdenCompra> compensar(OrdenCompra orden, List<ItemOrden> reservados, Throwable ex) {
        EstadoOrden estadoFinal = ex instanceof RiesgoAltoException ? EstadoOrden.RECHAZADA : EstadoOrden.COMPENSADA;
        return saga.liberarTodo(reservados, orden.getId())
                .then(Mono.defer(() -> {
                    orden.setEstado(estadoFinal);
                    orden.setExpiraEn(null);
                    return ordenes.save(orden);
                }))
                .doOnNext(o -> bus.publicar(EventoOrden.de(o.getId(), estadoFinal, ex.getMessage())))
                .then(Mono.error(ex));
    }

    // ---------- Consultar / confirmar ----------

    public Mono<OrdenCompra> obtener(Long id) {
        return ordenes.findById(id)
                .switchIfEmpty(Mono.error(new OrdenNoExisteException(id)))
                .flatMap(this::conItems);
    }

    private Mono<OrdenCompra> conItems(OrdenCompra orden) {
        return items.findByOrdenId(orden.getId())
                .collectList()
                .doOnNext(orden::setItems)
                .thenReturn(orden);
    }

    public Mono<OrdenCompra> confirmar(Long id) {
        return obtener(id)
                .flatMap(o -> o.getEstado() != EstadoOrden.RESERVADA
                        ? Mono.<OrdenCompra>error(new EstadoInvalidoException("La orden está en " + o.getEstado()))
                        : Flux.fromIterable(o.getItems())
                                .concatMap(i -> inventario.vender(i.getProductoId(), i.getCantidad(), id))
                                .then(Mono.defer(() -> {
                                    o.setEstado(EstadoOrden.CONFIRMADA);
                                    o.setExpiraEn(null);
                                    return ordenes.save(o);
                                }))
                                .doOnNext(saved -> saved.setItems(o.getItems()))
                                .as(tx::transactional))
                .doOnNext(o -> bus.publicar(EventoOrden.de(o.getId(), o.getEstado(), "Orden confirmada")));
    }

    // ---------- Stream por orden ----------

    public Flux<EventoOrden> eventos(Long id) {
        Flux<EventoOrden> heartbeat = Flux.interval(Duration.ofSeconds(15)).map(t -> EventoOrden.heartbeat(id));
        return obtener(id).flatMapMany(o -> {
            Mono<EventoOrden> actual = Mono.just(EventoOrden.de(o.getId(), o.getEstado(), "estado actual"));
            Flux<EventoOrden> vivo = bus.eventosOrden().filter(e -> id.equals(e.ordenId()));
            return Flux.merge(actual, vivo, heartbeat)
                    .takeUntil(e -> e.estado() != null && e.estado().esTerminal())
                    .doOnCancel(() -> log.info("Cliente cerró stream de orden {}", id));
        });
    }

    private static double redondear(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
