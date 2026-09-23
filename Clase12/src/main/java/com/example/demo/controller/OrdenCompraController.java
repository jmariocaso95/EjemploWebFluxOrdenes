package com.example.demo.controller;

import com.example.demo.dto.CrearOrdenRequest;
import com.example.demo.dto.EventoOrden;
import com.example.demo.model.OrdenCompra;
import com.example.demo.service.OrdenCompraService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Órdenes")
@RestController
@RequestMapping("/api/orders")
public class OrdenCompraController {

    private final OrdenCompraService service;

    public OrdenCompraController(OrdenCompraService service) {
        this.service = service;
    }

    @Operation(summary = "Crea una orden y reserva stock",
            description = "Persiste la orden, reserva stock por ítem con UPDATE ... RETURNING, consulta precio, "
                    + "impuesto y antifraude en paralelo y deja la orden en RESERVADA. Si algo falla, compensa.")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Orden creada en estado RESERVADA"),
            @ApiResponse(responseCode = "400", description = "Petición inválida (ValidacionException)"),
            @ApiResponse(responseCode = "404", description = "Algún producto no existe (ProductoNoExisteException)"),
            @ApiResponse(responseCode = "409", description = "Stock insuficiente (StockInsuficienteException)"),
            @ApiResponse(responseCode = "422", description = "Riesgo alto: reserva liberada (RiesgoAltoException)")})
    @PostMapping
    public Mono<ResponseEntity<OrdenCompra>> crear(
            @RequestBody CrearOrdenRequest request,
            @Parameter(description = "Repetir la misma clave devuelve la orden original sin descontar stock de nuevo",
                    example = "K1")
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        return service.crear(request, idempotencyKey)
                .map(o -> ResponseEntity.status(HttpStatus.CREATED).body(o));
    }

    @Operation(summary = "Consulta una orden con sus ítems")
    @ApiResponse(responseCode = "404", description = "OrdenNoExisteException")
    @GetMapping("/{id}")
    public Mono<OrdenCompra> obtener(@PathVariable Long id) {
        return service.obtener(id);
    }

    @Operation(summary = "Confirma la orden: el stock reservado pasa a vendido")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Orden CONFIRMADA"),
            @ApiResponse(responseCode = "404", description = "OrdenNoExisteException"),
            @ApiResponse(responseCode = "409", description = "La orden no está RESERVADA (EstadoInvalidoException)")})
    @PostMapping("/{id}/confirm")
    public Mono<OrdenCompra> confirmar(@PathVariable Long id) {
        return service.confirmar(id);
    }

    @Operation(summary = "Stream SSE de una orden",
            description = "Emite el estado actual y los cambios posteriores; cierra al llegar a un estado terminal.")
    @GetMapping(value = "/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<EventoOrden>> eventos(@PathVariable Long id) {
        return service.eventos(id)
                .map(e -> ServerSentEvent.builder(e)
                        .event(e.estado() == null ? "heartbeat" : "order")
                        .build());
    }
}
