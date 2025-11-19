package co.javeriana;

public interface LibroRepository {
    Libro findByCodigo(String codigo);
    void save(Libro libro);
}
