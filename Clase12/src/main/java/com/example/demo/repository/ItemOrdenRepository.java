package com.example.demo.repository;

import com.example.demo.model.ItemOrden;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface ItemOrdenRepository extends ReactiveCrudRepository<ItemOrden, Long> {

    Flux<ItemOrden> findByOrdenId(Long ordenId);

    /** El JOIN reemplaza la lista embebida de Mongo. Se consume en streaming, no se materializa. */
    @Query("""
           SELECT i.* FROM orden_item i
           JOIN orden_compra o ON o.id = i.orden_id
           WHERE o.estado = :estado
           """)
    Flux<ItemOrden> findByEstadoOrden(String estado);
}
