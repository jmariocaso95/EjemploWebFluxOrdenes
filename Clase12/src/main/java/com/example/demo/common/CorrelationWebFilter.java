package com.example.demo.common;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.UUID;

/**
 * Lee X-Correlation-Id (o genera uno) y lo pone en el Reactor Context.
 * Cualquier operador aguas abajo lo recupera con Mono.deferContextual sin recibirlo como parámetro.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationWebFilter implements WebFilter {

    public static final String KEY = "correlationId";
    public static final String HEADER = "X-Correlation-Id";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String cid = Optional.ofNullable(exchange.getRequest().getHeaders().getFirst(HEADER))
                .orElse(UUID.randomUUID().toString());
        exchange.getResponse().getHeaders().add(HEADER, cid);
        return chain.filter(exchange).contextWrite(ctx -> ctx.put(KEY, cid));
    }
}
