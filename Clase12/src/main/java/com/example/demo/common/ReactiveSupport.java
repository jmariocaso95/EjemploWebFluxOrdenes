package com.example.demo.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public final class ReactiveSupport {

    private static final Logger log = LoggerFactory.getLogger(ReactiveSupport.class);

    private ReactiveSupport() {}

    /** Backoff exponencial con jitter. Solo reintenta errores transitorios. Al agotar, propaga el error original. */
    public static Retry backoff(int attempts, Duration firstDelay) {
        return Retry.backoff(attempts, firstDelay)
                .jitter(0.5)
                .filter(ReactiveSupport::isTransient)
                .doBeforeRetry(s -> log.warn("Reintento #{} por {}", s.totalRetries() + 1, s.failure().toString()))
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }

    public static boolean isTransient(Throwable t) {
        return t instanceof TransientException
                || t instanceof TimeoutException
                || t instanceof TransientDataAccessException
                || t instanceof WebClientRequestException
                || (t instanceof WebClientResponseException w && w.getStatusCode().is5xxServerError());
    }

    /** Envuelve un paso con logs que incluyen el correlationId tomado del Context. */
    public static <T> Mono<T> traced(String step, Mono<T> source) {
        return Mono.deferContextual(ctx -> {
            String cid = ctx.getOrDefault(CorrelationWebFilter.KEY, "n/a");
            return source
                    .doOnSubscribe(s -> log.info("[{}] {} → inicio", cid, step))
                    .doOnSuccess(v -> log.info("[{}] {} → ok", cid, step))
                    .doOnError(e -> log.warn("[{}] {} → error {}", cid, step, e.toString()))
                    .doFinally(sig -> log.debug("[{}] {} → fin ({})", cid, step, sig));
        });
    }
}
