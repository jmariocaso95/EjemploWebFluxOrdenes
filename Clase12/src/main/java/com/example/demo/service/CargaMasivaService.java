package com.example.demo.service;

import com.example.demo.dto.ResultadoCarga;
import com.example.demo.model.Producto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
public class CargaMasivaService {

    private static final Logger log = LoggerFactory.getLogger(CargaMasivaService.class);

    private static final String UPSERT = """
            INSERT INTO productos (id, name, price, stock, category, reserved)
            VALUES (:id, :name, :price, :stock, :category, 0)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                price = EXCLUDED.price,
                stock = EXCLUDED.stock,
                category = EXCLUDED.category
            """;

    private final DatabaseClient db;
    private final TransactionalOperator tx;

    public CargaMasivaService(DatabaseClient db, TransactionalOperator tx) {
        this.db = db;
        this.tx = tx;
    }

    /** Entrada en streaming, lotes de 500, máximo 2 lotes concurrentes. */
    public Mono<ResultadoCarga> cargar(Flux<Producto> productos) {
        return productos
                .buffer(500)
                .flatMap(lote -> guardarLote(lote)
                        .map(n -> new ResultadoCarga(n, 0))
                        .onErrorResume(e -> {
                            log.error("Lote de {} productos falló: {}", lote.size(), e.toString());
                            return Mono.just(new ResultadoCarga(0, lote.size()));
                        }), 2)
                .reduce(new ResultadoCarga(0, 0), ResultadoCarga::mas);
    }

    private Mono<Integer> guardarLote(List<Producto> lote) {
        return Flux.fromIterable(lote)
                .concatMap(p -> db.sql(UPSERT)
                        .bind("id", p.getId())
                        .bind("name", p.getName())
                        .bind("price", p.getPrice())
                        .bind("stock", p.getStock())
                        .bind("category", p.getCategory() == null ? "SIN_CATEGORIA" : p.getCategory())
                        .fetch().rowsUpdated())
                .reduce(0L, Long::sum)
                .map(Long::intValue)
                .as(tx::transactional);
    }
}
