package co.javeriana;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class InMemoryLibroRepository implements LibroRepository {
    private final Map<String, Libro> libros = new ConcurrentHashMap<>();

    @Override
    public Libro findByCodigo(String codigo) {
        return libros.get(codigo);
    }

    @Override
    public void save(Libro libro) {
        libros.put(libro.getCodigo(), libro);
    }
}
