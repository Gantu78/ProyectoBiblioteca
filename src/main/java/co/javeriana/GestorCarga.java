package co.javeriana;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GestorCarga implements AutoCloseable {
    // Uso: mvn -q exec:java -Dexec.mainClass=co.javeriana.GestorCarga -Dexec.args="tcp://*:5555 tcp://*:5560"
    // Arg0 = endpoint REP para PS; Arg1 = endpoint PUB para Actores

    private final ZContext ctx;
    private ZMQ.Socket rep;
    private ZMQ.Socket pub;
    private ZMQ.Socket reqPrestamo;
    private final Queue<String> pendingPrestamos = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService retryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "gc-retry-thread");
        t.setDaemon(true);
        return t;
    });
    private long lamportClock = 0L;

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

        // Start retry task for queued PRESTAMO requests
        retryExecutor.scheduleWithFixedDelay(() -> {
            try {
                if (reqPrestamo == null) return;
                String carga;
                while ((carga = pendingPrestamos.poll()) != null) {
                    System.out.println("[GC-retry] Reintentando PRESTAMO desde cola: " + carga);
                    // attach lamport ts before sending
                    lamportClock++;
                    String cargaWithTs = carga + ";ts=" + lamportClock;
                    reqPrestamo.send(cargaWithTs.getBytes(ZMQ.CHARSET), 0);
                    byte[] r = reqPrestamo.recv(0);
                    String respuesta = r != null ? new String(r, ZMQ.CHARSET) : "ERROR:SinRespuesta";
                    System.out.println("[GC-retry] Respuesta: " + respuesta);
                    // If still GA_NoDisponible, re-enqueue with some backoff
                    if (respuesta.contains("GA_NoDisponible")) {
                        pendingPrestamos.add(carga);
                        Thread.sleep(1000);
                    } else {
                        System.out.println("[GC-retry] PRESTAMO procesado correctamente: " + respuesta);
                    }
                }
            } catch (Exception ex) {
                System.err.println("[GC-retry] Error durante reintento: " + ex.getMessage());
            }
        }, 5, 5, TimeUnit.SECONDS);

        while (!Thread.currentThread().isInterrupted()) {
            byte[] req = rep.recv(0); // bloqueante
            if (req == null) {
                continue;
            }

            String carga = new String(req, ZMQ.CHARSET);
            System.out.println("[GC] Recibido: " + carga);

            // Update Lamport clock on incoming PS message (PS doesn't send ts): increment local clock
            lamportClock++;

            if (carga.startsWith("DEVOLUCION") || carga.startsWith("RENOVACION")) {
                String topic = carga.startsWith("DEVOLUCION") ? "Devolucion" : "Renovacion";
                // ACK inmediato al PS
                rep.send("ACK".getBytes(ZMQ.CHARSET), 0);

                // Publicar a los Actores (frame 1 = tópico, frame 2 = carga)
                // attach ts to published payload
                String cargaWithTs = carga + ";ts=" + lamportClock;
                pub.sendMore(topic);
                pub.send(cargaWithTs);
                System.out.printf("[GC] Publicado -> topic=%s carga=%s%n", topic, carga);
                continue;
            }

            if (carga.startsWith("PRESTAMO")) {
                // Flujo síncrono: reenviar a ActorPrestamo vía REQ/REP y devolver su respuesta al PS
                if (reqPrestamo == null) {
                    rep.send("ERROR:NoActorPrestamoConfigured".getBytes(ZMQ.CHARSET), 0);
                    continue;
                }
                // attach lamport ts when forwarding
                lamportClock++;
                String cargaWithTs = carga + ";ts=" + lamportClock;
                reqPrestamo.send(cargaWithTs.getBytes(ZMQ.CHARSET), 0);
                byte[] resp = reqPrestamo.recv(0);
                String respuesta = resp != null ? new String(resp, ZMQ.CHARSET) : "ERROR:SinRespuesta";
                System.out.println("[GC] Respuesta ActorPrestamo -> " + respuesta);

                if (respuesta.contains("GA_NoDisponible")) {
                    // Encolar para reintento y notify PS with PENDING
                    pendingPrestamos.add(carga);
                    rep.send("PENDING".getBytes(ZMQ.CHARSET), 0);
                } else {
                    rep.send(respuesta.getBytes(ZMQ.CHARSET), 0);
                }
                continue;
            }

            // Operación desconocida
            rep.send("NACK:OperacionDesconocida".getBytes(ZMQ.CHARSET), 0);
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
        String repIP = args.length > 0 ? args[0] : "tcp://0.0.0.0:5555";
        String pubIP = args.length > 1 ? args[1] : "tcp://0.0.0.0:5560";
        String endpointActorPrestamo = args.length > 2 ? args[2] : null;

        try (GestorCarga gc = new GestorCarga()) {
            gc.iniciarRep(repIP);
            gc.iniciarPub(pubIP);
            if (endpointActorPrestamo != null) {
                gc.iniciarReqPrestamo(endpointActorPrestamo);
            }
            gc.runLoop();
        }
    }

    /** Conecta el socket REQ hacia ActorPrestamo (endpoint debe ser tcp://ip:puerto). */
    public void iniciarReqPrestamo(String endpointPrestamo) {
        reqPrestamo = ctx.createSocket(ZMQ.REQ);
        reqPrestamo.setLinger(0);
        reqPrestamo.connect(endpointPrestamo);
        System.out.println("[GC] REQ hacia ActorPrestamo en " + endpointPrestamo);
    }
}
