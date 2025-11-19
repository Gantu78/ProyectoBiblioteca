package co.javeriana;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.*;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class ActorRenovacion {
    // Uso: mvn -q exec:java -Dexec.mainClass=co.javeriana.ActorRenovacion -Dexec.args="tcp://IP_GC:5560"
    //
    // Documentación (español):
    // Este actor se suscribe a mensajes de renovación publicados por el GestorCarga.
    // - Inicializa GA primaria y réplica y un ReplicaManager.
    // - Usa `GestorAlmacenamientoConReplica` para aplicar renovaciones en primaria
    //   y replicarlas en segundo plano.
    // - Si la primaria falla (IllegalStateException), llama `rm.conmutarAReplica()`
    //   para que posteriores operaciones usen la réplica.
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

        // Pre-cargar un préstamo de ejemplo (id 102) en primaria si no existe
        if (primariaPrestamoRepo.findById("102") == null) {
            if (primariaLibroRepo.findByCodigo("L2") == null) primariaLibroRepo.save(new Libro("L2", "1984", "Orwell", 1));
            Prestamo p102 = new Prestamo("102", "U2", "L2", "2025-10-02", "2025-10-16", 0, PrestamoEstado.ACTIVO);
            primariaPrestamoRepo.save(p102);
        }

        // Cola persistente local (fallback) y REQ hacia GC para encolar centralmente
        String pendingPath = base + "primaria" + java.io.File.separator + "pending_renovaciones.db";
        DurableQueue pending = new DurableQueue(pendingPath);
        ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();

        String gcEnqueueEndpoint = args.length > 1 ? args[1] : null;
        ZMQ.Socket reqToGc;
        if (gcEnqueueEndpoint != null) {
            ZContext tmpCtx = new ZContext();
            reqToGc = tmpCtx.createSocket(ZMQ.REQ);
            reqToGc.setLinger(0);
            reqToGc.setReceiveTimeOut(2000);
            reqToGc.connect(gcEnqueueEndpoint);
            System.out.println("[ActorRenovacion] REQ hacia GC (enqueue) en " + gcEnqueueEndpoint);
        } else {
            reqToGc = null;
        }

        final ZMQ.Socket finalReqToGc = reqToGc;
        sched.scheduleAtFixedRate(() -> {
            try {
                pending.processAll(item -> {
                    if (finalReqToGc == null) return false;
                    try {
                        finalReqToGc.send(("ENQUEUE;type=Renovacion;carga=" + item).getBytes(ZMQ.CHARSET), 0);
                        byte[] r = finalReqToGc.recv(0);
                        String resp = r != null ? new String(r, ZMQ.CHARSET) : null;
                        if (resp != null && resp.contains("ENQUEUED")) {
                            System.out.println("[ActorRenovacion] Moved local item to central queue: " + item);
                            return true;
                        }
                    } catch (Exception ex) {
                        System.err.println("[ActorRenovacion] No se pudo enviar a GC en reprocessor: " + ex.getMessage());
                    }
                    return false;
                });
            } catch (Exception ex) {
                System.err.println("[ActorRenovacion] Error en reprocessor: " + ex.getMessage());
            }
        }, 5, 5, TimeUnit.SECONDS);

        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket sub = ctx.createSocket(ZMQ.SUB);
            sub.connect(pubIP);
            sub.subscribe("Renovacion".getBytes(ZMQ.CHARSET));
            // Suscribirse a eventos de control/failover publicados por el GC
            sub.subscribe("Failover".getBytes(ZMQ.CHARSET));
            System.out.println("[ActorRenovacion] SUB a " + pubIP + " (topic=Renovacion)");

            while (!Thread.currentThread().isInterrupted()) {
                String tema = sub.recvStr();
                String carga = sub.recvStr();
                System.out.printf("[ActorRenovacion] Mensaje recibido: tema=%s carga=%s%n", tema, carga);

                if ("Failover".equals(tema)) {
                    System.err.println("[ActorRenovacion] Evento Failover recibido: " + carga + "; solicitando conmutación a réplica");
                    rm.conmutarAReplica();
                    continue;
                }

                Long tsRemoto = Utils.extractTs(carga);
                if (tsRemoto != null) {
                    System.out.println("[ActorRenovacion] ts remoto=" + tsRemoto);
                }

                // parsear carga: RENOVACION;prestamoId=102;nuevaFecha=2025-10-22
                Map<String,String> kv = Utils.parseKeyValues(carga);
                String prestamoId = kv.get("prestamoId");
                String nuevaFecha = kv.get("nuevaFecha");

                if (prestamoId == null || nuevaFecha == null) {
                    System.out.println("[ActorRenovacion] Mensaje mal formado: falta prestamoId o nuevaFecha");
                    continue;
                }

                try {
                    boolean ok = gaCompuesto.registrarRenovacion(prestamoId, nuevaFecha);
                    System.out.println("[ActorRenovacion] Resultado GA = " + ok);
                    if (!ok) {
                        System.out.println("[ActorRenovacion] Operación rechazada por GA (no encolada)");
                    }
                } catch (IllegalStateException ex) {
                    System.err.println("[ActorRenovacion] GA primaria no disponible; intentando encolar en GC central");
                    boolean enqueuedCentral = false;
                    if (finalReqToGc != null) {
                        try {
                            finalReqToGc.send(("ENQUEUE;type=Renovacion;carga=" + carga).getBytes(ZMQ.CHARSET), 0);
                            byte[] r = finalReqToGc.recv(0);
                            String resp = r != null ? new String(r, ZMQ.CHARSET) : null;
                            if (resp != null && resp.contains("ENQUEUED")) {
                                enqueuedCentral = true;
                                System.out.println("[ActorRenovacion] Encolado en GC central: " + carga);
                            }
                        } catch (Exception rex) {
                            System.err.println("[ActorRenovacion] Error comunicando con GC: " + rex.getMessage());
                        }
                    }
                    if (!enqueuedCentral) {
                        System.err.println("[ActorRenovacion] Encolado local como fallback: " + carga);
                        pending.enqueue(carga);
                    }
                    rm.conmutarAReplica();
                }
            }
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { sched.shutdownNow(); } catch (Exception ignored) {}
            try { if (reqToGc != null) reqToGc.close(); } catch (Exception ignored) {}
        }));
    }
}
