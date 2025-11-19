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
                    // Si la primaria lanzó IllegalStateException, forzamos conmutación a réplica
                    System.err.println("[ActorPrestamo] GA primaria no disponible, solicitando conmutación a réplica");
                    rm.conmutarAReplica();
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
