package com.example.demo.dto;

/** Vista/patch de las perillas del simulador. Campos null = no cambiar. */
public record SimuladorConfig(Integer pricingFailures, Long fraudDelayMs, Integer forcedRiskScore) {}
