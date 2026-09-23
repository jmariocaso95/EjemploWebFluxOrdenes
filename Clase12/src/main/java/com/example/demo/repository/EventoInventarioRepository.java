package com.example.demo.repository;

import com.example.demo.model.EventoInventario;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

public interface EventoInventarioRepository extends ReactiveCrudRepository<EventoInventario, Long> {
}
