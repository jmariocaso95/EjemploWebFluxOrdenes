package com.example.demo.repository;

import com.example.demo.model.Producto;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface ProductReactiveRepository extends ReactiveCrudRepository<Producto, Long> {

    Flux<Producto> findByStockLessThan(int threshold);
}
