package com.example.demo.dto;

public record ResultadoCarga(int guardados, int fallidos) {
    public ResultadoCarga mas(ResultadoCarga otro) {
        return new ResultadoCarga(guardados + otro.guardados, fallidos + otro.fallidos);
    }
}
