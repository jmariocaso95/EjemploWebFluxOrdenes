package com.example.demo.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("orden_item")
public class ItemOrden {

    @Id
    private Long id;

    private Long ordenId;
    private Long productoId;
    private String categoria;
    private Integer cantidad;
    private Double precioUnitario;

    public ItemOrden() {}

    public ItemOrden(Long ordenId, Long productoId, String categoria, Integer cantidad, Double precioUnitario) {
        this.ordenId = ordenId;
        this.productoId = productoId;
        this.categoria = categoria;
        this.cantidad = cantidad;
        this.precioUnitario = precioUnitario;
    }

    /** Copia con otro precio unitario (los records serían inmutables; aquí lo hacemos explícito). */
    public ItemOrden conPrecio(Double nuevoPrecio) {
        return new ItemOrden(ordenId, productoId, categoria, cantidad, nuevoPrecio);
    }

    public ItemOrden conOrden(Long nuevaOrden) {
        return new ItemOrden(nuevaOrden, productoId, categoria, cantidad, precioUnitario);
    }

    public double totalLinea() {
        return (cantidad == null ? 0 : cantidad) * (precioUnitario == null ? 0.0 : precioUnitario);
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getOrdenId() { return ordenId; }
    public void setOrdenId(Long ordenId) { this.ordenId = ordenId; }
    public Long getProductoId() { return productoId; }
    public void setProductoId(Long productoId) { this.productoId = productoId; }
    public String getCategoria() { return categoria; }
    public void setCategoria(String categoria) { this.categoria = categoria; }
    public Integer getCantidad() { return cantidad; }
    public void setCantidad(Integer cantidad) { this.cantidad = cantidad; }
    public Double getPrecioUnitario() { return precioUnitario; }
    public void setPrecioUnitario(Double precioUnitario) { this.precioUnitario = precioUnitario; }
}
