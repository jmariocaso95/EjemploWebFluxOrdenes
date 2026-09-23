package com.example.demo.dto;

public record TotalCategoria(String categoria, long unidades, double monto) {

    public static TotalCategoria vacio(String categoria) {
        return new TotalCategoria(categoria, 0, 0.0);
    }

    public TotalCategoria mas(int cantidad, double montoLinea) {
        return new TotalCategoria(categoria, unidades + cantidad,
                Math.round((monto + montoLinea) * 100.0) / 100.0);
    }
}
