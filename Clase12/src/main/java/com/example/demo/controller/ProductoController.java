package com.example.demo.controller;


import com.example.demo.model.Producto;
import com.example.demo.repository.ProductReactiveRepository;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Productos")
@RestController
@RequestMapping("/api/products")
public class ProductoController {

    private final ProductReactiveRepository repository;

    public ProductoController(ProductReactiveRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public Flux<Producto> all() {
        return repository.findAll();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<Producto>> byId(@PathVariable Long id) {
        return repository.findById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PostMapping
    public Mono<Producto> create(@RequestBody Producto producto) {
        return repository.save(producto);
    }


    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Producto> stream() {
        return repository.findAll()
                .delayElements(Duration.ofSeconds(1));
    }
}
