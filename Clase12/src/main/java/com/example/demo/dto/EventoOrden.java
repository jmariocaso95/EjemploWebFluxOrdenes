package com.example.demo.dto;

import com.example.demo.model.EstadoOrden;

import java.time.Instant;

public record EventoOrden(Long ordenId, EstadoOrden estado, String mensaje, Instant timestamp) {

    public static EventoOrden de(Long ordenId, EstadoOrden estado, String mensaje) {
        return new EventoOrden(ordenId, estado, mensaje, Instant.now());
    }

    /** estado == null identifica un heartbeat. */
    public static EventoOrden heartbeat(Long ordenId) {
        return new EventoOrden(ordenId, null, "heartbeat", Instant.now());
    }
}
