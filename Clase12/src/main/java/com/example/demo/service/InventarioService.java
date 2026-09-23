package com.example.demo.service;

import com.example.demo.common.DomainExceptions.ProductoNoExisteException;
import com.example.demo.common.DomainExceptions.StockInsuficienteException;
import com.example.demo.model.EventoInventario;
import com.example.demo.model.Producto;
import com.example.demo.repository.EventoInventarioRepository;
import com.example.demo.repository.ProductReactiveRepository;
import io.r2dbc.spi.Readable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class InventarioService implements InventarioPort {

    private static final String SQL_RESERVAR = """
            UPDATE productos
               SET stock = stock - :cantidad,
                   reserved = reserved + :cantidad
             WHERE id = :id AND stock >= :cantidad
            RETURNING id, name, price, stock, reserved, category
            """;

    private static final String SQL_LIBERAR = """
            UPDATE productos
               SET stock = stock + :cantidad,
                   reserved = GREATEST(reserved - :cantidad, 0)
             WHERE id = :id
            """;

    private static final String SQL_VENDER = """
            UPDATE productos
               SET reserved = GREATEST(reserved - :cantidad, 0)
             WHERE id = :id
            """;

    private final DatabaseClient db;
    private final ProductReactiveRepository productos;
    private final EventoInventarioRepository eventos;
    private final EventBus bus;

    public InventarioService(DatabaseClient db, ProductReactiveRepository productos,
                             EventoInventarioRepository eventos, EventBus bus) {
        this.db = db;
        this.productos = productos;
        this.eventos = eventos;
        this.bus = bus;
    }

    @Override
    public Mono<Producto> reservar(Long productoId, int cantidad, Long ordenId) {
        return db.sql(SQL_RESERVAR)
                .bind("cantidad", cantidad)
                .bind("id", productoId)
                .map(InventarioService::aProducto)
                .one()
                .switchIfEmpty(Mono.defer(() -> productos.existsById(productoId)
                        .flatMap(existe -> Mono.<Producto>error(existe
                                ? new StockInsuficienteException(productoId, cantidad)
                                : new ProductoNoExisteException(productoId)))))
                .flatMap(p -> registrar("RESERVADO", productoId, -cantidad, ordenId).thenReturn(p));
    }

    @Override
    public Mono<Void> liberar(Long productoId, int cantidad, Long ordenId) {
        return db.sql(SQL_LIBERAR)
                .bind("cantidad", cantidad)
                .bind("id", productoId)
                .fetch().rowsUpdated()
                .then(registrar("LIBERADO", productoId, cantidad, ordenId));
    }

    @Override
    public Mono<Void> vender(Long productoId, int cantidad, Long ordenId) {
        return db.sql(SQL_VENDER)
                .bind("cantidad", cantidad)
                .bind("id", productoId)
                .fetch().rowsUpdated()
                .then(registrar("VENDIDO", productoId, 0, ordenId));
    }

    @Override
    public Flux<Producto> stockBajo(int threshold) {
        return productos.findByStockLessThan(threshold);
    }

    private Mono<Void> registrar(String tipo, Long productoId, int delta, Long ordenId) {
        return eventos.save(EventoInventario.de(tipo, productoId, delta, ordenId))
                .doOnNext(bus::publicar)
                .then();
    }

    private static Producto aProducto(Readable row) {
        Producto p = new Producto();
        p.setId(row.get("id", Long.class));
        p.setName(row.get("name", String.class));
        java.math.BigDecimal price = row.get("price", java.math.BigDecimal.class);
        p.setPrice(price == null ? null : price.doubleValue());
        p.setStock(row.get("stock", Integer.class));
        p.setReserved(row.get("reserved", Integer.class));
        p.setCategory(row.get("category", String.class));
        return p;
    }
}
