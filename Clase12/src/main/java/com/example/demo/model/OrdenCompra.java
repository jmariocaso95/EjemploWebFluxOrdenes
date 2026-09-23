package com.example.demo.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Table("orden_compra")
public class OrdenCompra {

    @Id
    private Long id;

    private Long clienteId;
    private String region;
    private String idempotencyKey;
    private EstadoOrden estado;
    private Double subtotal;
    private Double impuesto;
    private Double total;
    private Integer riskScore;
    private Instant creadoEn;
    private Instant expiraEn;

    /** R2DBC no mapea relaciones: los ítems se cargan aparte y se ensamblan aquí. */
    @Transient
    private List<ItemOrden> items = new ArrayList<>();

    public OrdenCompra() {}

    public static OrdenCompra nueva(Long clienteId, String region, String idempotencyKey) {
        OrdenCompra o = new OrdenCompra();
        o.clienteId = clienteId;
        o.region = region;
        o.idempotencyKey = idempotencyKey;
        o.estado = EstadoOrden.PENDIENTE;
        o.creadoEn = Instant.now();
        return o;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getClienteId() { return clienteId; }
    public void setClienteId(Long clienteId) { this.clienteId = clienteId; }
    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
    public EstadoOrden getEstado() { return estado; }
    public void setEstado(EstadoOrden estado) { this.estado = estado; }
    public Double getSubtotal() { return subtotal; }
    public void setSubtotal(Double subtotal) { this.subtotal = subtotal; }
    public Double getImpuesto() { return impuesto; }
    public void setImpuesto(Double impuesto) { this.impuesto = impuesto; }
    public Double getTotal() { return total; }
    public void setTotal(Double total) { this.total = total; }
    public Integer getRiskScore() { return riskScore; }
    public void setRiskScore(Integer riskScore) { this.riskScore = riskScore; }
    public Instant getCreadoEn() { return creadoEn; }
    public void setCreadoEn(Instant creadoEn) { this.creadoEn = creadoEn; }
    public Instant getExpiraEn() { return expiraEn; }
    public void setExpiraEn(Instant expiraEn) { this.expiraEn = expiraEn; }
    public List<ItemOrden> getItems() { return items; }
    public void setItems(List<ItemOrden> items) { this.items = items == null ? new ArrayList<>() : items; }
}
