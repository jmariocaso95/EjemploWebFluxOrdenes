package com.example.demo.service;

import com.example.demo.common.AppProperties;
import com.example.demo.common.ReactiveSupport;
import com.example.demo.dto.CotizacionPrecio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

@Component
public class ServiciosExternosClient implements ServiciosExternosPort {

    private static final Logger log = LoggerFactory.getLogger(ServiciosExternosClient.class);

    private final WebClient web;
    private final AppProperties props;
    private final Map<String, Mono<Double>> cacheImpuesto = new ConcurrentHashMap<>();

    public ServiciosExternosClient(WebClient externalWebClient, AppProperties props) {
        this.web = externalWebClient;
        this.props = props;
    }

    /** Timeout + retry con backoff. El fallback al catálogo lo decide OrdenCompraService. */
    @Override
    public Mono<CotizacionPrecio> precio(Long productoId) {
        return web.get().uri("/external/pricing/{id}", productoId)
                .retrieve()
                .bodyToMono(CotizacionPrecio.class)
                .timeout(props.external().pricingTimeout())
                .retryWhen(ReactiveSupport.backoff(3, Duration.ofMillis(200)));
    }

    /** Memoiza por región 10 min. Los errores no se cachean (ttl error = 0). */
    @Override
    public Mono<Double> tasaImpuesto(String region) {
        String key = region == null ? "DEFAULT" : region.toUpperCase();
        return cacheImpuesto.computeIfAbsent(key, r -> web.get().uri("/external/tax/{r}", r)
                .retrieve()
                .bodyToMono(Double.class)
                .doOnNext(rate -> log.info("Tasa de impuesto {} = {} (cacheada 10 min)", r, rate))
                .cache(v -> Duration.ofMinutes(10), e -> Duration.ZERO, () -> Duration.ZERO));
    }

    /** Timeout corto y degradación a score por defecto. */
    @Override
    public Mono<Integer> scoreRiesgo(String clienteId, double totalEstimado) {
        return web.post().uri("/external/fraud")
                .bodyValue(Map.of("clienteId", clienteId, "total", totalEstimado))
                .retrieve()
                .bodyToMono(Integer.class)
                .timeout(props.external().fraudTimeout())
                .doOnError(TimeoutException.class,
                        e -> log.warn("Antifraude excedió {}; usando score por defecto", props.external().fraudTimeout()))
                .onErrorReturn(TimeoutException.class, props.defaultRiskScore());
    }
}
