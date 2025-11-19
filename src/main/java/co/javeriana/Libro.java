package co.javeriana;

import java.io.Serializable;

public class Libro implements Serializable {
    private static final long serialVersionUID = 1L;
    private String codigo;
    private String titulo;
    private String autor;
    private int ejemplaresDisponibles;

    // No-arg constructor for JSON deserialization
    public Libro() {}

    public Libro(String codigo, String titulo, String autor, int ejemplaresDisponibles) {
        this.codigo = codigo;
        this.titulo = titulo;
        this.autor = autor;
        this.ejemplaresDisponibles = ejemplaresDisponibles;
    }

    public String getCodigo() { return codigo; }
    public void setCodigo(String codigo) { this.codigo = codigo; }

    public String getTitulo() { return titulo; }
    public void setTitulo(String titulo) { this.titulo = titulo; }

    public String getAutor() { return autor; }
    public void setAutor(String autor) { this.autor = autor; }

    public int getEjemplaresDisponibles() { return ejemplaresDisponibles; }
    public void setEjemplaresDisponibles(int ejemplaresDisponibles) { this.ejemplaresDisponibles = ejemplaresDisponibles; }
}
