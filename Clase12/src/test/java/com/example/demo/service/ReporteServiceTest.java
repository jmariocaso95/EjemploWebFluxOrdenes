package com.example.demo.service;

import com.example.demo.dto.TotalCategoria;
import com.example.demo.model.ItemOrden;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class ReporteServiceTest {

    private final ReporteService service = new ReporteService(null);

    @Test
    void agrupaPorCategoriaYOrdena() {
        Flux<ItemOrden> items = Flux.just(
                new ItemOrden(1L, 1L, "B", 2, 10.0),
                new ItemOrden(1L, 2L, "A", 1, 5.0),
                new ItemOrden(2L, 3L, "B", 1, 10.0),
                new ItemOrden(2L, 4L, null, 3, 1.0));

        StepVerifier.create(service.agregar(items))
                .assertNext(list -> {
                    assertThat(list).extracting(TotalCategoria::categoria).containsExactly("A", "B", "SIN_CATEGORIA");
                    assertThat(list.get(1).unidades()).isEqualTo(3);
                    assertThat(list.get(1).monto()).isEqualTo(30.0);
                })
                .verifyComplete();
    }

    @Test
    void fluxVacio_devuelveListaVacia() {
        StepVerifier.create(service.agregar(Flux.empty()))
                .assertNext(list -> assertThat(list).isEmpty())
                .verifyComplete();
    }
}
