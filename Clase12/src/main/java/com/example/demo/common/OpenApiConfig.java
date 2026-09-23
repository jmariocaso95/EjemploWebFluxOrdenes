package com.example.demo.common;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.parameters.HeaderParameter;
import io.swagger.v3.oas.models.tags.Tag;
import org.springdoc.core.customizers.OperationCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Metadatos del contrato OpenAPI. El documento se sirve en /v3/api-docs (JSON) y /v3/api-docs.yaml,
 * y se exporta a docs/openapi.{json,yaml} con `./gradlew exportOpenApi` para importarlo en Postman.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI demoOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Checkout reactivo sobre R2DBC")
                        .version("1.0.0")
                        .description("""
                                Órdenes de compra reactivas: reserva atómica de stock con saga de compensación,
                                servicios externos en paralelo (precio, impuesto, antifraude), confirmación,
                                streams SSE por orden y de tablero, reportes NDJSON y carga masiva de productos.

                                Estados de la orden: PENDIENTE → RESERVADA → CONFIRMADA. Terminales adicionales:
                                RECHAZADA (riesgo alto), COMPENSADA (fallo en la reserva), EXPIRADA (TTL vencido).

                                Errores: 400 ValidacionException · 404 ProductoNoExisteException /
                                OrdenNoExisteException · 409 StockInsuficienteException / EstadoInvalidoException ·
                                422 RiesgoAltoException. Todos devuelven { correlationId, timestamp, message, error }.""")
                        .contact(new Contact().name("Equipo demo"))
                        .license(new License().name("Uso interno")))
                .servers(List.of(new Server().url("http://localhost:8081").description("Local")))
                .tags(List.of(
                        new Tag().name("Órdenes").description("Alta, consulta, confirmación y stream SSE de órdenes"),
                        new Tag().name("Tablero").description("Stream SSE global compartido (hot publisher)"),
                        new Tag().name("Reportes").description("Ventas por categoría en JSON y acumulado NDJSON"),
                        new Tag().name("Productos").description("Catálogo y carga masiva NDJSON con upsert"),
                        new Tag().name("Clientes").description("CRUD reactivo de la demo previa"),
                        new Tag().name("Órdenes (demo CRUD)").description("Entidad `ordenes` de la demo previa"),
                        new Tag().name("Simulador externo").description("Controla fallos, latencia y riesgo de los servicios simulados")));
    }

    /** El CorrelationWebFilter lee este header en cualquier endpoint y lo devuelve en la respuesta. */
    @Bean
    public OperationCustomizer correlationIdHeader() {
        return (operation, handlerMethod) -> operation.addParametersItem(new HeaderParameter()
                .name("X-Correlation-Id")
                .description("Id de correlación propagado por Reactor Context hasta los logs y los errores. "
                        + "Si no se envía, la app genera uno y lo devuelve en la respuesta.")
                .required(false)
                .example("demo-1")
                .schema(new StringSchema()));
    }
}
