package com.example.demo.dto;

import java.util.List;

public record CrearOrdenRequest(Long clienteId, String region, List<Item> items) {
    public record Item(Long productoId, Integer cantidad) {}
}
