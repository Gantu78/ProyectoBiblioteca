package co.javeriana;

public interface PrestamoRepository {
    Prestamo findById(String id);
    void save(Prestamo p);
}
