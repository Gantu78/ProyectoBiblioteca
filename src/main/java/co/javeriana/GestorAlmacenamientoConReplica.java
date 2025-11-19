package co.javeriana;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * GestorAlmacenamientoConReplica agrupa una primaria y una réplica.
 *
 * Comportamiento mínimo implementado (fase A):
 * - Mientras la primaria esté activa, las operaciones se aplican primero en la primaria
 *   y se intentan replicar a la réplica (replicación asíncrona mediante un pool simple).
 * - Cuando la primaria se conmute a réplica, las operaciones se realizan únicamente
 *   contra la réplica.
 *
 * Notas:
 * - La replicación es best-effort en esta fase; en fases posteriores podemos añadir
 *   confirmación/retry y mecanismos de catch-up.
 */
public class GestorAlmacenamientoConReplica {
    private final GestorAlmacenamiento primaria;
    private final GestorAlmacenamiento replica;
    private final ReplicaManager rm;
    private volatile boolean primariaActiva = true;
    private final ExecutorService replicator = Executors.newFixedThreadPool(1);

    public GestorAlmacenamientoConReplica(GestorAlmacenamiento primaria, GestorAlmacenamiento replica, ReplicaManager rm) {
        this.primaria = primaria;
        this.replica = replica;
        this.rm = rm;
        this.primariaActiva = rm.primariaActiva();
    }

    public void setPrimariaActiva(boolean act) {
        this.primariaActiva = act;
    }

    // Registrar devolución: si primaria activa -> escribir en primaria y replicar en background.
    public synchronized boolean registrarDevolucion(String prestamoId) {
        if (!primariaActiva) {
            return replica.registrarDevolucion(prestamoId);
        }

        boolean ok = primaria.registrarDevolucion(prestamoId);
        // Replicar asíncronamente, best-effort
        replicator.submit(() -> {
            try {
                replica.registrarDevolucion(prestamoId);
            } catch (Exception ex) {
                System.err.println("[Replica] Error replicando devolucion: " + ex.getMessage());
            }
        });
        return ok;
    }

    public synchronized boolean registrarRenovacion(String prestamoId, String nuevaFecha) {
        if (!primariaActiva) {
            return replica.registrarRenovacion(prestamoId, nuevaFecha);
        }
        boolean ok = primaria.registrarRenovacion(prestamoId, nuevaFecha);
        replicator.submit(() -> {
            try {
                replica.registrarRenovacion(prestamoId, nuevaFecha);
            } catch (Exception ex) {
                System.err.println("[Replica] Error replicando renovacion: " + ex.getMessage());
            }
        });
        return ok;
    }

    public synchronized boolean validarDisponibilidad(String libroCodigo) {
        // Leer desde primaria si está activa, sino desde réplica
        if (!primariaActiva) return replica.validarDisponibilidad(libroCodigo);
        return primaria.validarDisponibilidad(libroCodigo);
    }

    public synchronized Prestamo otorgarPrestamo(String usuarioId, String libroCodigo, String fechaInicio, String fechaFin) {
        if (!primariaActiva) {
            return replica.otorgarPrestamo(usuarioId, libroCodigo, fechaInicio, fechaFin);
        }
        Prestamo p = primaria.otorgarPrestamo(usuarioId, libroCodigo, fechaInicio, fechaFin);
        replicator.submit(() -> {
            try {
                // Intentar replicar el efecto: recrear el prestamo en replica (si existe)
                if (p != null) {
                    replica.otorgarPrestamo(p.getUsuarioId(), p.getLibroCodigo(), p.getFechaInicio(), p.getFechaFin());
                }
            } catch (Exception ex) {
                System.err.println("[Replica] Error replicando otorgarPrestamo: " + ex.getMessage());
            }
        });
        return p;
    }

    public void shutdown() {
        replicator.shutdownNow();
    }
}
