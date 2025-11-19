package co.javeriana;

import java.io.Serializable;

public class Prestamo implements Serializable {
    private static final long serialVersionUID = 1L;
    private String id;
    private String usuarioId;
    private String libroCodigo;
    private String fechaInicio;
    private String fechaFin;
    private int renovaciones;
    private PrestamoEstado estado;

    // No-arg constructor for JSON deserialization
    public Prestamo() {}

    public Prestamo(String id, String usuarioId, String libroCodigo, String fechaInicio, String fechaFin, int renovaciones, PrestamoEstado estado) {
        this.id = id;
        this.usuarioId = usuarioId;
        this.libroCodigo = libroCodigo;
        this.fechaInicio = fechaInicio;
        this.fechaFin = fechaFin;
        this.renovaciones = renovaciones;
        this.estado = estado;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getUsuarioId() { return usuarioId; }
    public void setUsuarioId(String usuarioId) { this.usuarioId = usuarioId; }

    public String getLibroCodigo() { return libroCodigo; }
    public void setLibroCodigo(String libroCodigo) { this.libroCodigo = libroCodigo; }

    public String getFechaInicio() { return fechaInicio; }
    public void setFechaInicio(String fechaInicio) { this.fechaInicio = fechaInicio; }

    public String getFechaFin() { return fechaFin; }
    public void setFechaFin(String fechaFin) { this.fechaFin = fechaFin; }

    public int getRenovaciones() { return renovaciones; }
    public void setRenovaciones(int renovaciones) { this.renovaciones = renovaciones; }

    public PrestamoEstado getEstado() { return estado; }
    public void setEstado(PrestamoEstado estado) { this.estado = estado; }
}
