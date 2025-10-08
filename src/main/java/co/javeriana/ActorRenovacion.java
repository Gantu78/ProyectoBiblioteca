package co.javeriana;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.*;


public class ActorRenovacion {
    // Uso: mvn -q exec:java -Dexec.mainClass=co.javeriana.ActorRenovacion -Dexec.args="tcp://IP_GC:5560"
    public static void main(String[] args) {
        String pubIP = args.length > 0 ? args[0] : "tcp://localhost:5560";
        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket sub = ctx.createSocket(ZMQ.SUB);
            sub.connect(pubIP);
            sub.subscribe("Renovacion".getBytes(ZMQ.CHARSET));
            System.out.println("[ActorRenovacion] SUB a " + pubIP + " (topic=Renovacion)");

            while (!Thread.currentThread().isInterrupted()) {
                String tema = sub.recvStr();
                String carga = sub.recvStr();
                System.out.printf("[ActorRenovacion] Mensaje recibido: tema=%s carga=%s%n", tema, carga);

                // Simular actualización en BD (GA) para Entrega 1
                System.out.println("[ActorRenovacion] Actualizando BD: renovar fechaFin y aumentar contador (máx 2)...");
            }
        }
    }
}
