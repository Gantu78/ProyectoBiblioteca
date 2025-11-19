package co.javeriana;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class ActorPrestamo {
    // Uso: mvn -q exec:java -Dexec.mainClass=co.javeriana.ActorPrestamo -Dexec.args="tcp://*:5570"
    //
    // Documentación (español):
    // Este actor atiende solicitudes de PRESTAMO de forma síncrona (REP).
    // Implementación fase A de réplica/failover:
    // - Crea un GestorAlmacenamiento primario y otro réplica apuntando a
    //   `data/primaria` y `data/replica` respectivamente.
    // - Construye un `ReplicaManager` y obtiene el `GestorAlmacenamientoConReplica`
    //   para realizar operaciones. Mientras la primaria esté activa, las
    //   escrituras se aplican en primaria y se replican asíncronamente en la réplica.
    // - Si la primaria lanza `IllegalStateException` (simulando fallo), el actor
    //   solicita la conmutación a réplica llamando `rm.conmutarAReplica()` y
    //   responde `GA_NoDisponible` al GestorCarga.
    public static void main(String[] args) {
        String bind = args.length > 0 ? args[0] : "tcp://*:5570";

        // Crear GA primaria y réplica con rutas separadas (data/primaria, data/replica)
        String base = "data" + File.separator;
        String primariaPathLibros = base + "primaria" + File.separator + "libros.db";
        String primariaPathPrestamos = base + "primaria" + File.separator + "prestamos.db";
        String replicaPathLibros = base + "replica" + File.separator + "libros.db";
        String replicaPathPrestamos = base + "replica" + File.separator + "prestamos.db";

        FileBasedLibroRepository primariaLibroRepo = new FileBasedLibroRepository(primariaPathLibros);
        FileBasedPrestamoRepository primariaPrestamoRepo = new FileBasedPrestamoRepository(primariaPathPrestamos);
        GestorAlmacenamiento primariaGA = new GestorAlmacenamiento(primariaLibroRepo, primariaPrestamoRepo);

        FileBasedLibroRepository replicaLibroRepo = new FileBasedLibroRepository(replicaPathLibros);
        FileBasedPrestamoRepository replicaPrestamoRepo = new FileBasedPrestamoRepository(replicaPathPrestamos);
        GestorAlmacenamiento replicaGA = new GestorAlmacenamiento(replicaLibroRepo, replicaPrestamoRepo);

        // ReplicaManager y gestor compuesto
        ReplicaManager rm = new ReplicaManager(primariaGA, replicaGA);
        GestorAlmacenamientoConReplica gaCompuesto = rm.getActivo();

        // Soporte para simular fallo de primaria: pasar -DfailAfterN=10
        try {
            String s = System.getProperty("failAfterN");
            if (s != null) {
                int n = Integer.parseInt(s);
                if (n > 0) {
                    primariaGA.setFailAfter(n);
                }
            }
        } catch (Exception ex) {
            System.err.println("[ActorPrestamo] No se pudo parsear failAfterN: " + ex.getMessage());
        }

        // Lista de endpoints REP de los GCs remotos para notificar failover (comma-separated)
        String notifyGcEnqueue = System.getProperty("notifyGcEnqueue");
        String siteId = System.getProperty("siteId", "unknown");
        final String[] gcEnqueueEndpoints = notifyGcEnqueue != null ? notifyGcEnqueue.split(",") : new String[0];
        // Opcional: endpoints de GC remotos a los que reenviar operaciones cuando la primaria local falla
        String remoteGc = System.getProperty("remoteGcEndpoints");
        final String[] remoteGcEndpoints = remoteGc != null ? remoteGc.split(",") : new String[0];

        // DEBUG: log de propiedades leídas al inicio (ayuda a verificar ejecución desde IntelliJ/mvn)
        System.err.println("[ActorPrestamo] startup props: remoteGcEndpoints=" + (remoteGc == null ? "<none>" : remoteGc));
        System.err.println("[ActorPrestamo] startup props: notifyGcEnqueue=" + (notifyGcEnqueue == null ? "<none>" : notifyGcEnqueue));
        System.err.println("[ActorPrestamo] startup props: siteId=" + siteId);
        String sFail = System.getProperty("failAfterN");
        System.err.println("[ActorPrestamo] startup props: failAfterN=" + (sFail == null ? "<none>" : sFail));

        // Pre-cargar algunos libros en primaria si no existen
        if (primariaLibroRepo.findByCodigo("L1") == null) primariaLibroRepo.save(new Libro("L1", "El Quijote", "Cervantes", 2));
        if (primariaLibroRepo.findByCodigo("L2") == null) primariaLibroRepo.save(new Libro("L2", "1984", "Orwell", 1));

        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket rep = ctx.createSocket(ZMQ.REP);
            rep.bind(bind);
            System.out.println("[ActorPrestamo] REP en " + bind);

            long lamport = 0L;

            while (!Thread.currentThread().isInterrupted()) {
                String carga = rep.recvStr();
                if (carga == null) continue;
                System.out.println("[ActorPrestamo] Recibido: " + carga);

                // update lamport if incoming ts present
                Long tsRemoto = Utils.extractTs(carga);
                if (tsRemoto != null) lamport = Math.max(lamport, tsRemoto) + 1;
                else lamport++;

                // Formato: PRESTAMO;usuarioId=U1;libroCodigo=L1;inicio=2025-01-01;fin=2025-01-15
                Map<String,String> kv = Utils.parseKeyValues(carga);
                String usuarioId = kv.get("usuarioId");
                String libroCodigo = kv.get("libroCodigo");
                String inicio = kv.get("inicio");
                String fin = kv.get("fin");

                if (usuarioId == null || libroCodigo == null || inicio == null || fin == null) {
                    String resp = "ERROR;motivo=FormatoIncorrecto;ts=" + lamport;
                    rep.send(resp.getBytes(ZMQ.CHARSET), 0);
                    continue;
                }

                try {
                    // Usar el gestor compuesto (primaria + réplica)
                    Prestamo p = gaCompuesto.otorgarPrestamo(usuarioId, libroCodigo, inicio, fin);
                    if (p == null) {
                        String resp = "ERROR;motivo=SinDisponibilidad;ts=" + lamport;
                        rep.send(resp.getBytes(ZMQ.CHARSET), 0);
                    } else {
                        String resp = "OK;prestamoId=" + p.getId() + ";ts=" + lamport;
                        rep.send(resp.getBytes(ZMQ.CHARSET), 0);
                    }
                } catch (IllegalStateException ex) {
                    // Intentar reenviar la operación al/los GC remotos configurados antes de conmutar
                    boolean forwarded = false;
                    String forwardResp = null;
                    for (String remote : remoteGcEndpoints) {
                        try (ZContext tmp2 = new ZContext()) {
                            ZMQ.Socket req2 = tmp2.createSocket(ZMQ.REQ);
                            req2.setLinger(0);
                            req2.setReceiveTimeOut(2000);
                            req2.connect(remote);
                            req2.send(carga.getBytes(ZMQ.CHARSET), 0);
                            byte[] rr = req2.recv(0);
                            if (rr != null) {
                                forwardResp = new String(rr, ZMQ.CHARSET);
                                System.out.println("[ActorPrestamo] Reenviado PRESTAMO a GC remoto " + remote + " -> " + forwardResp);
                                forwarded = true;
                                break;
                            }
                        } catch (Exception e) {
                            System.err.println("[ActorPrestamo] Error reenviando a GC remoto " + remote + ": " + e.getMessage());
                        }
                    }

                    if (forwarded) {
                        // Enviar la respuesta del GC remoto al GC que llamó originalmente
                        rep.send(forwardResp.getBytes(ZMQ.CHARSET), 0);
                        continue;
                    }

                    // Ninguno remoto contestó: conmutar a réplica local y notificar GCs configurados
                    System.err.println("[ActorPrestamo] GA primaria no disponible, solicitando conmutación a réplica");
                    rm.conmutarAReplica();
                    String controlMsg = "site=" + siteId + ";event=FAILOVER";
                    for (String endpoint : gcEnqueueEndpoints) {
                        try (ZContext tmp = new ZContext()) {
                            ZMQ.Socket req = tmp.createSocket(ZMQ.REQ);
                            req.setLinger(0);
                            req.setReceiveTimeOut(2000);
                            req.connect(endpoint);
                            String payload = "ENQUEUE;type=Control;carga=" + controlMsg;
                            req.send(payload.getBytes(ZMQ.CHARSET), 0);
                            byte[] r = req.recv(0);
                            String rr = r != null ? new String(r, ZMQ.CHARSET) : "";
                            System.out.println("[ActorPrestamo] Notificado GC " + endpoint + " -> " + rr);
                        } catch (Exception e) {
                            System.err.println("[ActorPrestamo] Error notificando GC " + endpoint + ": " + e.getMessage());
                        }
                    }
                    String resp = "ERROR;motivo=GA_NoDisponible;ts=" + lamport;
                    rep.send(resp.getBytes(ZMQ.CHARSET), 0);
                }
            }
        }
    }

    private static Map<String,String> parseKeyValues(String carga) {
        Map<String,String> map = new HashMap<>();
        String[] parts = carga.split(";");
        for (int i = 1; i < parts.length; i++) {
            String part = parts[i];
            int eq = part.indexOf('=');
            if (eq > 0) {
                String k = part.substring(0, eq).trim();
                String v = part.substring(eq+1).trim();
                map.put(k, v);
            }
        }
        return map;
    }
}
