package com.example.demo.service;

import com.example.demo.dto.TotalCategoria;
import com.example.demo.model.EstadoOrden;
import com.example.demo.model.ItemOrden;
import com.example.demo.repository.ItemOrdenRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;

@Service
public class ReporteService {

    private final ItemOrdenRepository items;

    public ReporteService(ItemOrdenRepository items) {
        this.items = items;
    }

    /** limitRate(256): el driver R2DBC pide filas en lotes; nunca se materializa el resultado completo. */
    public Mono<List<TotalCategoria>> ventasPorCategoria() {
        return agregar(itemsConfirmados());
    }

    /** scan: total acumulado corriendo, emitido como NDJSON. */
    public Flux<TotalCategoria> totalCorriendo() {
        return itemsConfirmados()
                .scan(TotalCategoria.vacio("TOTAL"), (acc, i) -> acc.mas(i.getCantidad(), i.totalLinea()))
                .skip(1);
    }

    private Flux<ItemOrden> itemsConfirmados() {
        return items.findByEstadoOrden(EstadoOrden.CONFIRMADA.name())
                .limitRate(256);
    }

    /** Pura, sin I/O: fácil de probar con StepVerifier. */
    public Mono<List<TotalCategoria>> agregar(Flux<ItemOrden> fuente) {
        return fuente
                .groupBy(i -> i.getCategoria() == null ? "SIN_CATEGORIA" : i.getCategoria())
                .flatMap(grupo -> grupo.reduce(TotalCategoria.vacio(grupo.key()),
                        (acc, i) -> acc.mas(i.getCantidad(), i.totalLinea())))
                .collectSortedList(Comparator.comparing(TotalCategoria::categoria));
    }
}
