package co.javeriana;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class InMemoryPrestamoRepository implements PrestamoRepository {
    private final Map<String, Prestamo> prestamos = new ConcurrentHashMap<>();

    @Override
    public Prestamo findById(String id) {
        return prestamos.get(id);
    }

    @Override
    public void save(Prestamo p) {
        prestamos.put(p.getId(), p);
    }
}
