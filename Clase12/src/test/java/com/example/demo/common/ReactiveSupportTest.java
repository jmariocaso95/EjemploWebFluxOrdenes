package com.example.demo.common;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ReactiveSupportTest {

    @Test
    void backoff_reintentaErroresTransitoriosYLuegoEmite() {
        AtomicInteger calls = new AtomicInteger();

        StepVerifier.withVirtualTime(() -> Mono.defer(() -> calls.incrementAndGet() < 3
                                ? Mono.<String>error(new TransientException("boom"))
                                : Mono.just("ok"))
                        .retryWhen(ReactiveSupport.backoff(3, Duration.ofMillis(200))))
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(5))
                .expectNext("ok")
                .verifyComplete();

        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void backoff_noReintentaErroresNoTransitorios() {
        AtomicInteger calls = new AtomicInteger();

        StepVerifier.withVirtualTime(() -> Mono.defer(() -> {
                            calls.incrementAndGet();
                            return Mono.<String>error(new IllegalArgumentException("fatal"));
                        })
                        .retryWhen(ReactiveSupport.backoff(3, Duration.ofMillis(200))))
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(5))
                .expectError(IllegalArgumentException.class)
                .verify();

        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void timeout_degradaAValorPorDefecto() {
        StepVerifier.withVirtualTime(() -> Mono.<Integer>never()
                        .timeout(Duration.ofMillis(800))
                        .onErrorReturn(TimeoutException.class, 50))
                .expectSubscription()
                .thenAwait(Duration.ofSeconds(1))
                .expectNext(50)
                .verifyComplete();
    }
}
