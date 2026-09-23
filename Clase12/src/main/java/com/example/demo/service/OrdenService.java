package com.example.demo.service;

import com.example.demo.model.Orden;
import com.example.demo.repository.OrdenRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class OrdenService {
    private final OrdenRepository ordenRepository;

    public OrdenService(OrdenRepository ordenRepository) {
        this.ordenRepository = ordenRepository;
    }

    public Flux<Orden> getAllOrdenes() {
        return ordenRepository.findAll();
    }

    public Flux<Orden> getOrdenesByClienteId(Long clienteId) {
        return ordenRepository.findByClienteId(clienteId);
    }

    public Mono<Orden> saveOrden(Orden orden) {
        return ordenRepository.save(orden);
    }

    public Mono<Void> deleteOrden(Long id) {
        return ordenRepository.deleteById(id);
    }

}
