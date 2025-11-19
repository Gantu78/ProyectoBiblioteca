package co.javeriana;

import java.util.UUID;

public class GestorAlmacenamiento {
    private final LibroRepository libroRepo;
    private final PrestamoRepository prestamoRepo;
    private volatile boolean disponible = true;
    // Simulación de fallo: si failAfter > 0, tras 'failAfter' operaciones de escritura
    // la primaria se marcará no disponible. opCount incrementa tras cada operación.
    private volatile int failAfter = -1;
    private volatile int opCount = 0;

    public GestorAlmacenamiento(LibroRepository libroRepo, PrestamoRepository prestamoRepo) {
        this.libroRepo = libroRepo;
        this.prestamoRepo = prestamoRepo;
    }

    public void setDisponible(boolean disponible) {
        this.disponible = disponible;
    }

    public void setFailAfter(int n) {
        this.failAfter = n;
        this.opCount = 0;
        if (n > 0) System.err.println("[GestorAlmacenamiento] Modo fallo activado: fallar después de " + n + " operaciones");
    }

    private void maybeTriggerFail() {
        if (failAfter > 0 && opCount >= failAfter) {
            System.err.println("[GestorAlmacenamiento] Simulando fallo de primaria tras " + opCount + " operaciones");
            this.disponible = false;
        }
    }

    private void checkDisponible() {
        if (!disponible) throw new IllegalStateException("GA no disponible");
    }

    public synchronized boolean registrarDevolucion(String prestamoId) {
        maybeTriggerFail();
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
        // Contabilizar operación exitosa
        opCount++;
        return true;
    }

    public synchronized boolean registrarRenovacion(String prestamoId, String nuevaFecha) {
        maybeTriggerFail();
        checkDisponible();
        Prestamo p = prestamoRepo.findById(prestamoId);
        if (p == null) return false;

        if (p.getRenovaciones() >= 2) return false;

        p.setRenovaciones(p.getRenovaciones() + 1);
        p.setFechaFin(nuevaFecha);
        prestamoRepo.save(p);
        opCount++;
        return true;
    }

    public synchronized boolean validarDisponibilidad(String libroCodigo) {
        maybeTriggerFail();
        checkDisponible();
        Libro l = libroRepo.findByCodigo(libroCodigo);
        return l != null && l.getEjemplaresDisponibles() > 0;
    }

    public synchronized Prestamo otorgarPrestamo(String usuarioId, String libroCodigo, String fechaInicio, String fechaFin) {
        maybeTriggerFail();
        checkDisponible();
        if (!validarDisponibilidad(libroCodigo)) return null;

        Libro l = libroRepo.findByCodigo(libroCodigo);
        l.setEjemplaresDisponibles(l.getEjemplaresDisponibles() - 1);
        libroRepo.save(l);

        String id = UUID.randomUUID().toString();
        Prestamo p = new Prestamo(id, usuarioId, libroCodigo, fechaInicio, fechaFin, 0, PrestamoEstado.ACTIVO);
        prestamoRepo.save(p);
        opCount++;
        return p;
    }
}
