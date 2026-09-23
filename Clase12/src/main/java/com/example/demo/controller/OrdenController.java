package com.example.demo.controller;

import com.example.demo.model.Orden;
import com.example.demo.service.OrdenService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Órdenes (demo CRUD)")
@RestController
@RequestMapping("/api/ordenes")
public class OrdenController {
    @Autowired
    private OrdenService ordenService;

    @GetMapping
    public Flux<Orden> getAllOrdenes() {
        return ordenService.getAllOrdenes();
    }

    @GetMapping("/cliente/{clienteId}")
    public Flux<Orden> getOrdenesByClienteId(@PathVariable Long clienteId) {
        return ordenService.getOrdenesByClienteId(clienteId);
    }

    @PostMapping
    public Mono<Orden> saveOrden(@RequestBody Orden orden) {
        return ordenService.saveOrden(orden);
    }
}
