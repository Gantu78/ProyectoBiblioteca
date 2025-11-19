package co.javeriana;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.*;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class ActorDevolucion {
    // Uso: mvn -q exec:java -Dexec.mainClass=co.javeriana.ActorDevolucion -Dexec.args="tcp://IP_GC:5560"
    //
    // Documentación (español):
    // Este actor se suscribe a mensajes de devolución publicados por el GestorCarga.
    // Implementación fase A de réplica/failover:
    // - Crea GA primaria y réplica (rutas `data/primaria` y `data/replica`).
    // - Usa `ReplicaManager` y `GestorAlmacenamientoConReplica` para aplicar la
    //   operación de devolución en la primaria y replicarla asíncronamente.
    // - Si la primaria lanza `IllegalStateException`, el actor solicita la
    //   conmutación a réplica mediante `rm.conmutarAReplica()` para que las
    //   siguientes operaciones se apliquen en la réplica.
    public static void main(String[] args) {
        String pubIP = args.length > 0 ? args[0] : "tcp://localhost:5560";
        // Crear GA primaria y réplica (rutas separadas)
        String base = "data" + java.io.File.separator;
        String primariaPathLibros = base + "primaria" + java.io.File.separator + "libros.db";
        String primariaPathPrestamos = base + "primaria" + java.io.File.separator + "prestamos.db";
        String replicaPathLibros = base + "replica" + java.io.File.separator + "libros.db";
        String replicaPathPrestamos = base + "replica" + java.io.File.separator + "prestamos.db";

        FileBasedLibroRepository primariaLibroRepo = new FileBasedLibroRepository(primariaPathLibros);
        FileBasedPrestamoRepository primariaPrestamoRepo = new FileBasedPrestamoRepository(primariaPathPrestamos);
        GestorAlmacenamiento primariaGA = new GestorAlmacenamiento(primariaLibroRepo, primariaPrestamoRepo);

        FileBasedLibroRepository replicaLibroRepo = new FileBasedLibroRepository(replicaPathLibros);
        FileBasedPrestamoRepository replicaPrestamoRepo = new FileBasedPrestamoRepository(replicaPathPrestamos);
        GestorAlmacenamiento replicaGA = new GestorAlmacenamiento(replicaLibroRepo, replicaPrestamoRepo);

        ReplicaManager rm = new ReplicaManager(primariaGA, replicaGA);
        GestorAlmacenamientoConReplica gaCompuesto = rm.getActivo();

        // Pre-cargar un préstamo de ejemplo (id 101) en primaria si no existe
        if (primariaPrestamoRepo.findById("101") == null) {
            if (primariaLibroRepo.findByCodigo("L1") == null) primariaLibroRepo.save(new Libro("L1", "El Quijote", "Cervantes", 1));
            Prestamo p101 = new Prestamo("101", "U1", "L1", "2025-10-01", "2025-10-15", 0, PrestamoEstado.ACTIVO);
            primariaPrestamoRepo.save(p101);
        }

        // Cola persistente local (fallback) y REQ hacia GC para encolar centralmente
        String pendingPath = base + "primaria" + java.io.File.separator + "pending_devoluciones.db";
        DurableQueue pending = new DurableQueue(pendingPath);
        ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();

        // REQ hacia GC actor-enqueue (opcional). Segundo arg: endpoint GC actor enqueue (e.g. tcp://localhost:5556)
        String gcEnqueueEndpoint = args.length > 1 ? args[1] : null;
        ZMQ.Socket reqToGc;
        if (gcEnqueueEndpoint != null) {
            ZContext tmpCtx = new ZContext();
            reqToGc = tmpCtx.createSocket(ZMQ.REQ);
            reqToGc.setLinger(0);
            reqToGc.setReceiveTimeOut(2000);
            reqToGc.connect(gcEnqueueEndpoint);
            System.out.println("[ActorDevolucion] REQ hacia GC (enqueue) en " + gcEnqueueEndpoint);
        } else {
            reqToGc = null;
        }

        // Opcional: endpoints de GC remotos a los que reenviar operaciones cuando la primaria local falla
        String remoteGc = System.getProperty("remoteGcEndpoints");
        final String[] remoteGcEndpoints = remoteGc != null ? remoteGc.split(",") : new String[0];

        // Reprocesador periódico: intentar enviar localmente los items al GC central (si configurado)
        final ZMQ.Socket finalReqToGc = reqToGc;
        sched.scheduleAtFixedRate(() -> {
            try {
                pending.processAll(item -> {
                    if (finalReqToGc == null) return false;
                    try {
                        finalReqToGc.send(("ENQUEUE;type=Devolucion;carga=" + item).getBytes(ZMQ.CHARSET), 0);
                        byte[] r = finalReqToGc.recv(0);
                        String resp = r != null ? new String(r, ZMQ.CHARSET) : null;
                        if (resp != null && resp.contains("ENQUEUED")) {
                            System.out.println("[ActorDevolucion] Moved local item to central queue: " + item);
                            return true;
                        }
                    } catch (Exception ex) {
                        System.err.println("[ActorDevolucion] No se pudo enviar a GC en reprocessor: " + ex.getMessage());
                    }
                    return false;
                });
            } catch (Exception ex) {
                System.err.println("[ActorDevolucion] Error en reprocessor: " + ex.getMessage());
            }
        }, 5, 5, TimeUnit.SECONDS);

        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket sub = ctx.createSocket(ZMQ.SUB);
            sub.connect(pubIP);
            sub.subscribe("Devolucion".getBytes(ZMQ.CHARSET));
            // Suscribirse a eventos de control/failover publicados por el GC
            sub.subscribe("Failover".getBytes(ZMQ.CHARSET));
            System.out.println("[ActorDevolucion] SUB a " + pubIP + " (topic=Devolucion)");

            while (!Thread.currentThread().isInterrupted()) {
                String tema = sub.recvStr();
                String carga = sub.recvStr();
                System.out.printf("[ActorDevolucion] Mensaje recibido: tema=%s carga=%s%n", tema, carga);

                // Si es un evento de Failover, activar conmutación local
                if ("Failover".equals(tema)) {
                    System.err.println("[ActorDevolucion] Evento Failover recibido: " + carga + "; solicitando conmutación a réplica");
                    rm.conmutarAReplica();
                    continue;
                }

                // Update lamport clock if ts present
                Long tsRemoto = Utils.extractTs(carga);
                // (actor-local lamport not persisted currently) - just log
                if (tsRemoto != null) {
                    System.out.println("[ActorDevolucion] ts remoto=" + tsRemoto);
                }

                // parsear carga: DEVOLUCION;prestamoId=101
                String prestamoId = null;
                Map<String,String> kv = Utils.parseKeyValues(carga);
                prestamoId = kv.get("prestamoId");

                if (prestamoId == null) {
                    System.out.println("[ActorDevolucion] Mensaje mal formado: no se encontró prestamoId");
                    continue;
                }

                try {
                    boolean ok = gaCompuesto.registrarDevolucion(prestamoId);
                    System.out.println("[ActorDevolucion] Resultado GA = " + ok);
                    if (!ok) {
                        System.out.println("[ActorDevolucion] Operación rechazada por GA (no encolada)");
                    }
                } catch (IllegalStateException ex) {
                    System.err.println("[ActorDevolucion] GA primaria no disponible; intentando reenviar a GC remoto");
                    boolean forwarded = false;
                    for (String remote : remoteGcEndpoints) {
                        try (ZContext tmp2 = new ZContext()) {
                            ZMQ.Socket req2 = tmp2.createSocket(ZMQ.REQ);
                            req2.setLinger(0);
                            req2.setReceiveTimeOut(2000);
                            req2.connect(remote);
                            req2.send(carga.getBytes(ZMQ.CHARSET), 0);
                            byte[] r = req2.recv(0);
                            String resp = r != null ? new String(r, ZMQ.CHARSET) : null;
                            if (resp != null) {
                                System.out.println("[ActorDevolucion] Reenviado a GC remoto " + remote + " -> " + resp);
                                forwarded = true;
                                break;
                            }
                        } catch (Exception e) {
                            System.err.println("[ActorDevolucion] Error reenviando a GC remoto " + remote + ": " + e.getMessage());
                        }
                    }

                    if (forwarded) continue;

                    System.err.println("[ActorDevolucion] Intentando encolar en GC central");
                    boolean enqueuedCentral = false;
                    if (finalReqToGc != null) {
                        try {
                            finalReqToGc.send(("ENQUEUE;type=Devolucion;carga=" + carga).getBytes(ZMQ.CHARSET), 0);
                            byte[] r = finalReqToGc.recv(0);
                            String resp = r != null ? new String(r, ZMQ.CHARSET) : null;
                            if (resp != null && resp.contains("ENQUEUED")) {
                                enqueuedCentral = true;
                                System.out.println("[ActorDevolucion] Encolado en GC central: " + carga);
                            }
                        } catch (Exception rex) {
                            System.err.println("[ActorDevolucion] Error comunicando con GC: " + rex.getMessage());
                        }
                    }
                    if (!enqueuedCentral) {
                        System.err.println("[ActorDevolucion] Encolado local como fallback: " + carga);
                        pending.enqueue(carga);
                    }
                    rm.conmutarAReplica();
                }
            }
        }
        // Añadir shutdown hook para cerrar scheduler
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { sched.shutdownNow(); } catch (Exception ignored) {}
            try { if (reqToGc != null) reqToGc.close(); } catch (Exception ignored) {}
        }));
    }
}
