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
    // Uso: mvn -q exec:java -Dexec.mainClass=co.javeriana.GestorCarga -Dexec.args="tcp://*:5555 tcp://*:5560 tcp://actorPrestamo:5570 tcp://*:5556"
    // Arg0 = endpoint REP para PS
    // Arg1 = endpoint PUB para Actores
    // Arg2 = endpoint REQ hacia ActorPrestamo (opcional) - ej. tcp://localhost:5570
    // Arg3 = endpoint REP para que Actores encolen fallos en GC (opcional) - ej. tcp://*:5556

    private final ZContext ctx;
    private ZMQ.Socket rep;
    private ZMQ.Socket pub;
    private ZMQ.Socket reqPrestamo;
    private ZMQ.Socket repActorEnqueue;
    private DurableQueue centralPendingDevoluciones;
    private DurableQueue centralPendingRenovaciones;
    private DurableQueue centralPendingPrestamos;
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

    /** Exponer endpoint REP para que actores puedan pedir encolar operaciones fallidas. */
    public void iniciarRepActorEnqueue(String bind) {
        repActorEnqueue = ctx.createSocket(ZMQ.REP);
        repActorEnqueue.setLinger(0);
        repActorEnqueue.bind(bind);
        System.out.println("[GC] REP (actor-enqueue) escuchando en " + bind);

        // Inicializar colas durables centrales
        String base = "data" + java.io.File.separator + "gc" + java.io.File.separator;
        centralPendingDevoluciones = new DurableQueue(base + "pending_devoluciones.db");
        centralPendingRenovaciones = new DurableQueue(base + "pending_renovaciones.db");
    }

    /** Bucle principal: recibe por REP, responde ACK y publica por PUB. */
    public void runLoop() {
        if (rep == null || pub == null) {
            throw new IllegalStateException("Llama primero a iniciarRep() e iniciarPub().");
        }

        // Ensure central pending prestamo queue exists
        try {
            String base = "data" + java.io.File.separator + "gc" + java.io.File.separator;
            centralPendingPrestamos = new DurableQueue(base + "pending_prestamos.db");
        } catch (Exception ex) {
            System.err.println("[GC] No se pudo inicializar centralPendingPrestamos: " + ex.getMessage());
        }

        // Start retry task for queued PRESTAMO requests and to reprocesar colas centrales
        retryExecutor.scheduleWithFixedDelay(() -> {
            try {
                if (reqPrestamo == null) return;

                // First, process durable central pending prestamos
                if (centralPendingPrestamos != null) {
                    centralPendingPrestamos.processAll(item -> {
                        try {
                            System.out.println("[GC-retry] Reintentando PRESTAMO desde cola central: " + item);
                            lamportClock++;
                            String cargaWithTs = item + ";ts=" + lamportClock;
                            reqPrestamo.send(cargaWithTs.getBytes(ZMQ.CHARSET), 0);
                            byte[] r = reqPrestamo.recv(0);
                            String respuesta = r != null ? new String(r, ZMQ.CHARSET) : "ERROR:SinRespuesta";
                            System.out.println("[GC-retry] Respuesta: " + respuesta);
                            if (respuesta.contains("GA_NoDisponible")) {
                                return false; // keep in queue
                            } else {
                                System.out.println("[GC-retry] PRESTAMO procesado correctamente: " + respuesta);
                                return true; // remove from durable queue
                            }
                        } catch (Exception ex) {
                            System.err.println("[GC-retry] Error procesando pedido desde cola central: " + ex.getMessage());
                            return false;
                        }
                    });
                }

                // Also process in-memory pendingPrestamos (fallback)
                String carga;
                while ((carga = pendingPrestamos.poll()) != null) {
                    System.out.println("[GC-retry] Reintentando PRESTAMO desde cola en memoria: " + carga);
                    lamportClock++;
                    String cargaWithTs = carga + ";ts=" + lamportClock;
                    reqPrestamo.send(cargaWithTs.getBytes(ZMQ.CHARSET), 0);
                    byte[] r = reqPrestamo.recv(0);
                    String respuesta = r != null ? new String(r, ZMQ.CHARSET) : "ERROR:SinRespuesta";
                    System.out.println("[GC-retry] Respuesta: " + respuesta);
                    if (respuesta.contains("GA_NoDisponible")) {
                        // move to durable queue for longer-term persistence
                        if (centralPendingPrestamos != null) centralPendingPrestamos.enqueue(carga);
                        else pendingPrestamos.add(carga);
                        Thread.sleep(1000);
                    } else {
                        System.out.println("[GC-retry] PRESTAMO procesado correctamente: " + respuesta);
                    }
                }
                // Reprocesar colas centrales: publicar nuevamente los mensajes (Devolucion/Renovacion)
                if (centralPendingDevoluciones != null) {
                    centralPendingDevoluciones.processAll(item -> {
                        try {
                            lamportClock++;
                            String cargaWithTs = item + ";ts=" + lamportClock;
                            pub.sendMore("Devolucion");
                            pub.send(cargaWithTs);
                            System.out.println("[GC-retry] Re-publicada Devolucion desde cola central: " + cargaWithTs);
                            return true; // eliminado de la cola local central
                        } catch (Exception ex) {
                            System.err.println("[GC-retry] Error re-publicando Devolucion: " + ex.getMessage());
                            return false;
                        }
                    });
                }
                if (centralPendingRenovaciones != null) {
                    centralPendingRenovaciones.processAll(item -> {
                        try {
                            lamportClock++;
                            String cargaWithTs = item + ";ts=" + lamportClock;
                            pub.sendMore("Renovacion");
                            pub.send(cargaWithTs);
                            System.out.println("[GC-retry] Re-publicada Renovacion desde cola central: " + cargaWithTs);
                            return true;
                        } catch (Exception ex) {
                            System.err.println("[GC-retry] Error re-publicando Renovacion: " + ex.getMessage());
                            return false;
                        }
                    });
                }
            } catch (Exception ex) {
                System.err.println("[GC-retry] Error durante reintento: " + ex.getMessage());
            }
        }, 5, 5, TimeUnit.SECONDS);

        // Poll both PS REP and actor-enqueue REP (if configured)
        ZMQ.Poller poller = ctx.createPoller(2);
        poller.register(rep, ZMQ.Poller.POLLIN);
        int idxActorRep = -1;
        if (repActorEnqueue != null) {
            poller.register(repActorEnqueue, ZMQ.Poller.POLLIN);
            idxActorRep = 1;
        }

        while (!Thread.currentThread().isInterrupted()) {
            int rc = poller.poll();
            if (rc == 0) continue;

            String carga = null;
            if (poller.pollin(0)) {
                byte[] req = rep.recv(0); // bloqueante
                if (req == null) continue;
                carga = new String(req, ZMQ.CHARSET);
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

                // actor-enqueue requests
                if (idxActorRep >= 0 && poller.pollin(idxActorRep)) {
                    byte[] reqA = repActorEnqueue.recv(0);
                    if (reqA == null) continue;
                    String msg = new String(reqA, ZMQ.CHARSET);
                    System.out.println("[GC] Recibido desde actor (enqueue): " + msg);
                    // Esperamos una carga del formato: ENQUEUE;type=Devolucion;carga=...
                    if (msg.startsWith("ENQUEUE;")) {
                        // parse key-values after ENQUEUE;
                        String payload = msg.substring("ENQUEUE;".length());
                        java.util.Map<String, String> kv = Utils.parseKeyValues(payload);
                        String type = kv.get("type");
                        String cargaOrig = kv.get("carga");
                        if (type == null || cargaOrig == null) {
                            repActorEnqueue.send("ERROR:Malformed".getBytes(ZMQ.CHARSET), 0);
                        } else {
                            if (type.equalsIgnoreCase("Devolucion") && centralPendingDevoluciones != null) {
                                centralPendingDevoluciones.enqueue(cargaOrig);
                                repActorEnqueue.send("ENQUEUED".getBytes(ZMQ.CHARSET), 0);
                            } else if (type.equalsIgnoreCase("Renovacion") && centralPendingRenovaciones != null) {
                                centralPendingRenovaciones.enqueue(cargaOrig);
                                repActorEnqueue.send("ENQUEUED".getBytes(ZMQ.CHARSET), 0);
                            } else {
                                repActorEnqueue.send("ERROR:UnknownType".getBytes(ZMQ.CHARSET), 0);
                            }
                        }
                    } else {
                        repActorEnqueue.send("ERROR:Unsupported".getBytes(ZMQ.CHARSET), 0);
                    }
                }
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
                    // Encolar para reintento y notify PS with PENDING (persistir en cola central)
                    if (centralPendingPrestamos != null) {
                        centralPendingPrestamos.enqueue(carga);
                    } else {
                        pendingPrestamos.add(carga);
                    }
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
        String endpointActorEnqueue = args.length > 3 ? args[3] : null;

        try (GestorCarga gc = new GestorCarga()) {
            gc.iniciarRep(repIP);
            gc.iniciarPub(pubIP);
            if (endpointActorPrestamo != null) {
                gc.iniciarReqPrestamo(endpointActorPrestamo);
            }
            if (endpointActorEnqueue != null) {
                gc.iniciarRepActorEnqueue(endpointActorEnqueue);
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
