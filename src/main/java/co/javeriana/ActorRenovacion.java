package co.javeriana;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.*;

import java.util.Map;


public class ActorRenovacion {
    // Uso: mvn -q exec:java -Dexec.mainClass=co.javeriana.ActorRenovacion -Dexec.args="tcp://IP_GC:5560"
    public static void main(String[] args) {
        String pubIP = args.length > 0 ? args[0] : "tcp://localhost:5560";
        // Crear GA con repositorios persistentes en archivos (data/)
        String dir = "data";
        FileBasedLibroRepository libroRepo = new FileBasedLibroRepository(dir + java.io.File.separator + "libros.db");
        FileBasedPrestamoRepository prestamoRepo = new FileBasedPrestamoRepository(dir + java.io.File.separator + "prestamos.db");
        GestorAlmacenamiento ga = new GestorAlmacenamiento(libroRepo, prestamoRepo);

        // Pre-cargar un préstamo de ejemplo (id 102) si no existe
        if (prestamoRepo.findById("102") == null) {
            if (libroRepo.findByCodigo("L2") == null) libroRepo.save(new Libro("L2", "1984", "Orwell", 1));
            Prestamo p102 = new Prestamo("102", "U2", "L2", "2025-10-02", "2025-10-16", 0, PrestamoEstado.ACTIVO);
            prestamoRepo.save(p102);
        }

        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket sub = ctx.createSocket(ZMQ.SUB);
            sub.connect(pubIP);
            sub.subscribe("Renovacion".getBytes(ZMQ.CHARSET));
            System.out.println("[ActorRenovacion] SUB a " + pubIP + " (topic=Renovacion)");

            while (!Thread.currentThread().isInterrupted()) {
                String tema = sub.recvStr();
                String carga = sub.recvStr();
                System.out.printf("[ActorRenovacion] Mensaje recibido: tema=%s carga=%s%n", tema, carga);

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
                    boolean ok = ga.registrarRenovacion(prestamoId, nuevaFecha);
                    System.out.println("[ActorRenovacion] Resultado GA = " + ok);
                } catch (IllegalStateException ex) {
                    System.out.println("[ActorRenovacion] GA no disponible: encolando/reintentando (no implementado)");
                }
            }
        }
    }
}
