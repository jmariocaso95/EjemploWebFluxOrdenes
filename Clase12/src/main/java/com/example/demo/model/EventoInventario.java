package com.example.demo.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Table("evento_inventario")
public class EventoInventario {

    @Id
    private Long id;

    private String tipo;          // RESERVADO | LIBERADO | VENDIDO
    private Long productoId;
    private Integer delta;
    private Long ordenId;
    private Instant ocurridoEn;

    public EventoInventario() {}

    public static EventoInventario de(String tipo, Long productoId, int delta, Long ordenId) {
        EventoInventario e = new EventoInventario();
        e.tipo = tipo;
        e.productoId = productoId;
        e.delta = delta;
        e.ordenId = ordenId;
        e.ocurridoEn = Instant.now();
        return e;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getTipo() { return tipo; }
    public void setTipo(String tipo) { this.tipo = tipo; }
    public Long getProductoId() { return productoId; }
    public void setProductoId(Long productoId) { this.productoId = productoId; }
    public Integer getDelta() { return delta; }
    public void setDelta(Integer delta) { this.delta = delta; }
    public Long getOrdenId() { return ordenId; }
    public void setOrdenId(Long ordenId) { this.ordenId = ordenId; }
    public Instant getOcurridoEn() { return ocurridoEn; }
    public void setOcurridoEn(Instant ocurridoEn) { this.ocurridoEn = ocurridoEn; }
}
