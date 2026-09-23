package com.example.demo.service;

import com.example.demo.common.DomainExceptions.ProductoNoExisteException;
import com.example.demo.common.DomainExceptions.StockInsuficienteException;
import com.example.demo.model.ItemOrden;
import com.example.demo.model.Producto;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

class ReservaSagaTest {

    /** Inventario en memoria. Permite inyectar un Mono personalizado por producto (para TestPublisher). */
    static class FakeInventario implements InventarioPort {
        final Map<Long, Producto> productos = new ConcurrentHashMap<>();
        final List<String> log = new CopyOnWriteArrayList<>();
        Function<Long, Mono<Producto>> override = id -> null;

        FakeInventario con(long id, int stock, double price, String category) {
            Producto p = new Producto();
            p.setId(id); p.setStock(stock); p.setPrice(price); p.setCategory(category); p.setName("P" + id);
            productos.put(id, p);
            return this;
        }

        @Override public Mono<Producto> reservar(Long id, int cantidad, Long ordenId) {
            Mono<Producto> custom = override.apply(id);
            if (custom != null) return custom;
            return Mono.defer(() -> {
                Producto p = productos.get(id);
                if (p == null) return Mono.error(new ProductoNoExisteException(id));
                if (p.getStock() < cantidad) return Mono.error(new StockInsuficienteException(id, cantidad));
                p.setStock(p.getStock() - cantidad);
                log.add("reservar:" + id);
                return Mono.just(p);
            });
        }

        @Override public Mono<Void> liberar(Long id, int cantidad, Long ordenId) {
            return Mono.fromRunnable(() -> {
                Producto p = productos.get(id);
                p.setStock(p.getStock() + cantidad);
                log.add("liberar:" + id);
            });
        }

        @Override public Mono<Void> vender(Long id, int cantidad, Long ordenId) { return Mono.empty(); }
        @Override public Flux<Producto> stockBajo(int threshold) { return Flux.empty(); }
    }

    @Test
    void reservaTodo_yEnriqueceConCategoriaYPrecio() {
        FakeInventario inv = new FakeInventario()
                .con(1, 10, 100.0, "A")
                .con(2, 5, 20.0, "B");
        ReservaSaga saga = new ReservaSaga(inv);

        List<ItemOrden> items = List.of(
                new ItemOrden(7L, 1L, null, 2, null),
                new ItemOrden(7L, 2L, null, 1, null));

        StepVerifier.create(saga.reservarTodo(items, 7L))
                .assertNext(result -> {
                    assertThat(result).hasSize(2);
                    assertThat(result.get(0).getCategoria()).isEqualTo("A");
                    assertThat(result.get(0).getPrecioUnitario()).isEqualTo(100.0);
                    assertThat(result.get(0).getOrdenId()).isEqualTo(7L);
                })
                .verifyComplete();

        assertThat(inv.productos.get(1L).getStock()).isEqualTo(8);
        assertThat(inv.log).containsExactly("reservar:1", "reservar:2");
    }

    @Test
    void fallaSegundoItem_compensaElPrimero() {
        FakeInventario inv = new FakeInventario()
                .con(1, 10, 100.0, "A")
                .con(2, 0, 20.0, "B");
        ReservaSaga saga = new ReservaSaga(inv);

        List<ItemOrden> items = List.of(
                new ItemOrden(7L, 1L, null, 2, null),
                new ItemOrden(7L, 2L, null, 1, null));

        StepVerifier.create(saga.reservarTodo(items, 7L))
                .expectError(StockInsuficienteException.class)
                .verify();

        assertThat(inv.productos.get(1L).getStock()).isEqualTo(10);
        assertThat(inv.log).containsExactly("reservar:1", "liberar:1");
    }

    @Test
    void conTestPublisher_errorTardioTambienCompensa() {
        FakeInventario inv = new FakeInventario().con(1, 10, 100.0, "A").con(2, 5, 20.0, "B");
        TestPublisher<Producto> lento = TestPublisher.create();
        inv.override = id -> id == 2L ? lento.mono() : null;
        ReservaSaga saga = new ReservaSaga(inv);

        List<ItemOrden> items = List.of(
                new ItemOrden(7L, 1L, null, 3, null),
                new ItemOrden(7L, 2L, null, 1, null));

        StepVerifier.create(saga.reservarTodo(items, 7L))
                .then(() -> {
                    assertThat(inv.log).containsExactly("reservar:1"); // el 1 ya reservó, el 2 sigue pendiente
                    lento.error(new StockInsuficienteException(2L, 1));
                })
                .expectError(StockInsuficienteException.class)
                .verify();

        assertThat(inv.productos.get(1L).getStock()).isEqualTo(10);
        assertThat(inv.log).containsExactly("reservar:1", "liberar:1");
    }
}
