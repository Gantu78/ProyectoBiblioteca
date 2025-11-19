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
            System.err.println("[GestorAlmacenamiento] Simulando fallo de primaria tras " + opCount + " operaciones (failAfter=" + failAfter + ")");
            this.disponible = false;
        }
    }

    private void checkDisponible() {
        if (!disponible) throw new IllegalStateException("GA no disponible");
    }

    public synchronized boolean registrarDevolucion(String prestamoId) {
        System.err.println("[GestorAlmacenamiento] registrarDevolucion start: opCount=" + opCount + " failAfter=" + failAfter + " disponible=" + disponible);
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
        System.err.println("[GestorAlmacenamiento] registrarDevolucion done: new opCount=" + opCount);
        // After counting this write, possibly trigger the simulated failure immediately
        maybeTriggerFail();
        return true;
    }

    public synchronized boolean registrarRenovacion(String prestamoId, String nuevaFecha) {
        System.err.println("[GestorAlmacenamiento] registrarRenovacion start: opCount=" + opCount + " failAfter=" + failAfter + " disponible=" + disponible);
        checkDisponible();
        Prestamo p = prestamoRepo.findById(prestamoId);
        if (p == null) return false;

        if (p.getRenovaciones() >= 2) return false;

        p.setRenovaciones(p.getRenovaciones() + 1);
        p.setFechaFin(nuevaFecha);
        prestamoRepo.save(p);
        opCount++;
        System.err.println("[GestorAlmacenamiento] registrarRenovacion done: new opCount=" + opCount);
        maybeTriggerFail();
        return true;
    }

    public synchronized boolean validarDisponibilidad(String libroCodigo) {
        System.err.println("[GestorAlmacenamiento] validarDisponibilidad start: opCount=" + opCount + " failAfter=" + failAfter + " disponible=" + disponible);
        checkDisponible();
        Libro l = libroRepo.findByCodigo(libroCodigo);
        if (l == null) {
            System.err.println("[GestorAlmacenamiento] validarDisponibilidad: libro not found -> " + libroCodigo);
            return false;
        }
        System.err.println("[GestorAlmacenamiento] validarDisponibilidad: libro=" + l.getCodigo() + " ejemplares=" + l.getEjemplaresDisponibles());
        return l.getEjemplaresDisponibles() > 0;
    }

    public synchronized Prestamo otorgarPrestamo(String usuarioId, String libroCodigo, String fechaInicio, String fechaFin) {
        System.err.println("[GestorAlmacenamiento] otorgarPrestamo start: opCount=" + opCount + " failAfter=" + failAfter + " disponible=" + disponible + " libro=" + libroCodigo + " usuario=" + usuarioId);
        checkDisponible();
        if (!validarDisponibilidad(libroCodigo)) return null;

        Libro l = libroRepo.findByCodigo(libroCodigo);
        l.setEjemplaresDisponibles(l.getEjemplaresDisponibles() - 1);
        libroRepo.save(l);

        String id = UUID.randomUUID().toString();
        Prestamo p = new Prestamo(id, usuarioId, libroCodigo, fechaInicio, fechaFin, 0, PrestamoEstado.ACTIVO);
        prestamoRepo.save(p);
        opCount++;
        System.err.println("[GestorAlmacenamiento] otorgarPrestamo done: new opCount=" + opCount + " prestamoId=" + id);
        maybeTriggerFail();
        return p;
    }
}
