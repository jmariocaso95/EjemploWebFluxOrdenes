package com.example.demo.common;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("app")
public record AppProperties(
        External external,
        Duration reservationTtl,
        Duration expiryInterval,
        int riskThreshold,
        int defaultRiskScore,
        int lowStockThreshold) {

    public record External(String baseUrl, Duration pricingTimeout, Duration fraudTimeout) {}
}
