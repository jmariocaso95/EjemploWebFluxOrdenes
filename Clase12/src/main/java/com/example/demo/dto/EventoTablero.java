package com.example.demo.dto;

import java.time.Instant;

public record EventoTablero(String tipo, Object payload, Instant timestamp) {
    public static EventoTablero de(String tipo, Object payload) {
        return new EventoTablero(tipo, payload, Instant.now());
    }
}
