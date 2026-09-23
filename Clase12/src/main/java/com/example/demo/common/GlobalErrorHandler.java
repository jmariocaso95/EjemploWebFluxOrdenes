package com.example.demo.common;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;

@RestControllerAdvice
public class GlobalErrorHandler {

    @ExceptionHandler(DomainException.class)
    public Mono<ResponseEntity<Map<String, Object>>> domain(DomainException ex) {
        return Mono.deferContextual(ctx -> {
            Map<String, Object> body = Map.of(
                    "error", ex.getClass().getSimpleName(),
                    "message", ex.getMessage(),
                    "correlationId", ctx.getOrDefault(CorrelationWebFilter.KEY, "n/a"),
                    "timestamp", Instant.now().toString());
            return Mono.just(ResponseEntity.status(ex.status()).body(body));
        });
    }
}
