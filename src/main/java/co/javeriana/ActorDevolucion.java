package co.javeriana;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.*;

import java.util.Map;


public class ActorDevolucion {
    // Uso: mvn -q exec:java -Dexec.mainClass=co.javeriana.ActorDevolucion -Dexec.args="tcp://IP_GC:5560"
    public static void main(String[] args) {
        String pubIP = args.length > 0 ? args[0] : "tcp://localhost:5560";
        // Crear GA con repositorios persistentes en archivos (data/)
        String dir = "data";
        FileBasedLibroRepository libroRepo = new FileBasedLibroRepository(dir + java.io.File.separator + "libros.db");
        FileBasedPrestamoRepository prestamoRepo = new FileBasedPrestamoRepository(dir + java.io.File.separator + "prestamos.db");
        GestorAlmacenamiento ga = new GestorAlmacenamiento(libroRepo, prestamoRepo);

        // Pre-cargar un préstamo de ejemplo (id 101) si no existe
        if (prestamoRepo.findById("101") == null) {
            if (libroRepo.findByCodigo("L1") == null) libroRepo.save(new Libro("L1", "El Quijote", "Cervantes", 1));
            Prestamo p101 = new Prestamo("101", "U1", "L1", "2025-10-01", "2025-10-15", 0, PrestamoEstado.ACTIVO);
            prestamoRepo.save(p101);
        }

        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket sub = ctx.createSocket(ZMQ.SUB);
            sub.connect(pubIP);
            sub.subscribe("Devolucion".getBytes(ZMQ.CHARSET));
            System.out.println("[ActorDevolucion] SUB a " + pubIP + " (topic=Devolucion)");

            while (!Thread.currentThread().isInterrupted()) {
                String tema = sub.recvStr();
                String carga = sub.recvStr();
                System.out.printf("[ActorDevolucion] Mensaje recibido: tema=%s carga=%s%n", tema, carga);

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
                    boolean ok = ga.registrarDevolucion(prestamoId);
                    System.out.println("[ActorDevolucion] Resultado GA = " + ok);
                } catch (IllegalStateException ex) {
                    System.out.println("[ActorDevolucion] GA no disponible: encolando/reintentando (no implementado)");
                }
            }
        }
    }
}
