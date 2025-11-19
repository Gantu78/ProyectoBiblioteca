package co.javeriana;

import java.util.UUID;

public class GestorAlmacenamiento {
    private final LibroRepository libroRepo;
    private final PrestamoRepository prestamoRepo;
    private volatile boolean disponible = true;

    public GestorAlmacenamiento(LibroRepository libroRepo, PrestamoRepository prestamoRepo) {
        this.libroRepo = libroRepo;
        this.prestamoRepo = prestamoRepo;
    }

    public void setDisponible(boolean disponible) {
        this.disponible = disponible;
    }

    private void checkDisponible() {
        if (!disponible) throw new IllegalStateException("GA no disponible");
    }

    public synchronized boolean registrarDevolucion(String prestamoId) {
        checkDisponible();
        Prestamo p = prestamoRepo.findById(prestamoId);
        if (p == null) return false;

        p.setEstado(PrestamoEstado.DEVUELTO);
        prestamoRepo.save(p);

        Libro l = libroRepo.findByCodigo(p.getLibroCodigo());
        if (l != null) {
            l.setEjemplaresDisponibles(l.getEjemplaresDisponibles() + 1);
            libroRepo.save(l);
        }
        return true;
    }

    public synchronized boolean registrarRenovacion(String prestamoId, String nuevaFecha) {
        checkDisponible();
        Prestamo p = prestamoRepo.findById(prestamoId);
        if (p == null) return false;

        if (p.getRenovaciones() >= 2) return false;

        p.setRenovaciones(p.getRenovaciones() + 1);
        p.setFechaFin(nuevaFecha);
        prestamoRepo.save(p);
        return true;
    }

    public synchronized boolean validarDisponibilidad(String libroCodigo) {
        checkDisponible();
        Libro l = libroRepo.findByCodigo(libroCodigo);
        return l != null && l.getEjemplaresDisponibles() > 0;
    }

    public synchronized Prestamo otorgarPrestamo(String usuarioId, String libroCodigo, String fechaInicio, String fechaFin) {
        checkDisponible();
        if (!validarDisponibilidad(libroCodigo)) return null;

        Libro l = libroRepo.findByCodigo(libroCodigo);
        l.setEjemplaresDisponibles(l.getEjemplaresDisponibles() - 1);
        libroRepo.save(l);

        String id = UUID.randomUUID().toString();
        Prestamo p = new Prestamo(id, usuarioId, libroCodigo, fechaInicio, fechaFin, 0, PrestamoEstado.ACTIVO);
        prestamoRepo.save(p);
        return p;
    }
}
