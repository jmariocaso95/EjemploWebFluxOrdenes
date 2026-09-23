package com.example.demo.model;

public enum EstadoOrden {
    PENDIENTE, RESERVADA, CONFIRMADA, RECHAZADA, EXPIRADA, COMPENSADA;

    public boolean esTerminal() {
        return this == CONFIRMADA || this == RECHAZADA || this == EXPIRADA || this == COMPENSADA;
    }
}
