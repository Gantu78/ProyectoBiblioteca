package co.javeriana;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.*;



public class ActorDevolucion {
    // Uso: mvn -q exec:java -Dexec.mainClass=co.javeriana.ActorDevolucion -Dexec.args="tcp://IP_GC:5560"
    public static void main(String[] args) {
        String pubIP = args.length > 0 ? args[0] : "tcp://localhost:5560";
        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket sub = ctx.createSocket(ZMQ.SUB);
            sub.connect(pubIP);
            sub.subscribe("Devolucion".getBytes(ZMQ.CHARSET));
            System.out.println("[ActorDevolucion] SUB a " + pubIP + " (topic=Devolucion)");

            while (!Thread.currentThread().isInterrupted()) {
                String tema = sub.recvStr();
                String carga = sub.recvStr();
                System.out.printf("[ActorDevolucion] Mensaje recibido: tema=%s carga=%s%n", tema, carga);

                // Simular actualización en BD (GA)
                System.out.println("[ActorDevolucion] Actualizando BD: marcar prestamo como DEVUELTO y sumar ejemplar...");
            }
        }
    }
}
