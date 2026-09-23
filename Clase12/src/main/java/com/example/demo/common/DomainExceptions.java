package com.example.demo.common;

import org.springframework.http.HttpStatus;

public final class DomainExceptions {

    private DomainExceptions() {}

    public static class ValidacionException extends DomainException {
        public ValidacionException(String msg) { super(HttpStatus.BAD_REQUEST, msg); }
    }

    public static class ProductoNoExisteException extends DomainException {
        public ProductoNoExisteException(Long id) { super(HttpStatus.NOT_FOUND, "Producto no existe: " + id); }
    }

    public static class StockInsuficienteException extends DomainException {
        public StockInsuficienteException(Long id, int cantidad) {
            super(HttpStatus.CONFLICT, "Stock insuficiente para producto " + id + " (cantidad " + cantidad + ")");
        }
    }

    public static class RiesgoAltoException extends DomainException {
        public RiesgoAltoException(int score) { super(HttpStatus.UNPROCESSABLE_ENTITY, "Riesgo alto: " + score); }
    }

    public static class OrdenNoExisteException extends DomainException {
        public OrdenNoExisteException(Long id) { super(HttpStatus.NOT_FOUND, "Orden no existe: " + id); }
    }

    public static class EstadoInvalidoException extends DomainException {
        public EstadoInvalidoException(String msg) { super(HttpStatus.CONFLICT, msg); }
    }
}
