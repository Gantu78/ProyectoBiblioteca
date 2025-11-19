package co.javeriana;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class ActorPrestamo {
    // Uso: mvn -q exec:java -Dexec.mainClass=co.javeriana.ActorPrestamo -Dexec.args="tcp://*:5570"
    public static void main(String[] args) {
        String bind = args.length > 0 ? args[0] : "tcp://*:5570";

    // Crear GA con repositorios persistentes en archivos (data/)
    String dir = "data";
    FileBasedLibroRepository libroRepo = new FileBasedLibroRepository(dir + File.separator + "libros.db");
    FileBasedPrestamoRepository prestamoRepo = new FileBasedPrestamoRepository(dir + File.separator + "prestamos.db");
    GestorAlmacenamiento ga = new GestorAlmacenamiento(libroRepo, prestamoRepo);

    // Pre-cargar algunos libros si no existen
    if (libroRepo.findByCodigo("L1") == null) libroRepo.save(new Libro("L1", "El Quijote", "Cervantes", 2));
    if (libroRepo.findByCodigo("L2") == null) libroRepo.save(new Libro("L2", "1984", "Orwell", 1));

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
                    Prestamo p = ga.otorgarPrestamo(usuarioId, libroCodigo, inicio, fin);
                    if (p == null) {
                        String resp = "ERROR;motivo=SinDisponibilidad;ts=" + lamport;
                        rep.send(resp.getBytes(ZMQ.CHARSET), 0);
                    } else {
                        String resp = "OK;prestamoId=" + p.getId() + ";ts=" + lamport;
                        rep.send(resp.getBytes(ZMQ.CHARSET), 0);
                    }
                } catch (IllegalStateException ex) {
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
