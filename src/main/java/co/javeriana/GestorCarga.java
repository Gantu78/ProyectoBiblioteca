package co.javeriana;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;

public class GestorCarga implements AutoCloseable {
    // Uso: mvn -q exec:java -Dexec.mainClass=co.javeriana.GestorCarga -Dexec.args="tcp://*:5555 tcp://*:5560"
    // Arg0 = endpoint REP para PS; Arg1 = endpoint PUB para Actores

    private final ZContext ctx;
    private ZMQ.Socket rep;
    private ZMQ.Socket pub;

    public GestorCarga() {
        this.ctx = new ZContext();
    }

    /** Crea y bindea el socket REP (para atender PS). */
    public void iniciarRep(String repBind) {
        rep = ctx.createSocket(ZMQ.REP);
        
        rep.setReceiveTimeOut(0);     // bloqueante
        rep.setLinger(0);             // no retener mensajes al cerrar
        rep.bind(repBind);
        System.out.println("[GC] REP escuchando en " + repBind);
    }

    /** Crea y bindea el socket PUB (para publicar a Actores). */
    public void iniciarPub(String pubBind) {
        pub = ctx.createSocket(ZMQ.PUB);
        pub.setLinger(0);
        pub.bind(pubBind);
        System.out.println("[GC] PUB publicando en " + pubBind);
    }

    /** Bucle principal: recibe por REP, responde ACK y publica por PUB. */
    public void runLoop() {
        if (rep == null || pub == null) {
            throw new IllegalStateException("Llama primero a iniciarRep() e iniciarPub().");
        }

        while (!Thread.currentThread().isInterrupted()) {
            byte[] req = rep.recv(0); // bloqueante
            if (req == null) {
                continue;
            }

            String carga = new String(req, ZMQ.CHARSET);
            System.out.println("[GC] Recibido: " + carga);

            String topic;
            if (carga.startsWith("DEVOLUCION"))      topic = "Devolucion";
            else if (carga.startsWith("RENOVACION"))  topic = "Renovacion";
            else {
                rep.send("NACK:OperacionDesconocida".getBytes(ZMQ.CHARSET), 0);
                continue;
            }

            // Responder ACK inmediato al PS
            rep.send("ACK".getBytes(ZMQ.CHARSET), 0);

            // Publicar a los Actores (frame 1 = tópico, frame 2 = carga)
            pub.sendMore(topic);
            pub.send(carga);
            System.out.printf("[GC] Publicado -> topic=%s carga=%s%n", topic, carga);
        }
    }

    @Override
    public void close() {
        // Cierre ordenado
        if (rep != null) rep.close();
        if (pub != null) pub.close();
        ctx.close();
        System.out.println("[GC] Contexto y sockets cerrados.");
    }

    public static void main(String[] args) {
        String repIP = args.length > 0 ? args[0] : "tcp://*:5555";
        String pubIP = args.length > 1 ? args[1] : "tcp://*:5560";

        try (GestorCarga gc = new GestorCarga()) {
            gc.iniciarRep(repIP);
            gc.iniciarPub(pubIP);
            gc.runLoop();
        }
    }
}
