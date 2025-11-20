package co.javeriana;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Gestor de Carga Multihilo con Pool de Workers
 * Optimizado para alta concurrencia y rendimiento en pruebas de carga
 */
public class GestorCarga implements AutoCloseable {

    private static final int NUM_WORKERS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int QUEUE_CAPACITY = 10000;

    private final ZContext ctx;
    private ZMQ.Socket rep;
    private ZMQ.Socket pub;
    private ZMQ.Socket repActorEnqueue;

    // Thread-safe collections
    private DurableQueue centralPendingDevoluciones;
    private DurableQueue centralPendingRenovaciones;
    private DurableQueue centralPendingPrestamos;
    private final BlockingQueue<PrestamoRequest> prestamoQueue;

    // Thread pools
    private final ExecutorService workerPool;
    private final ExecutorService prestamoWorkerPool;
    private final ScheduledExecutorService retryExecutor;

    // Lamport clock thread-safe
    private final AtomicLong lamportClock = new AtomicLong(0L);

    // Control de shutdown
    private volatile boolean running = true;

    // Métricas
    private final AtomicLong requestsProcessed = new AtomicLong(0);
    private final AtomicLong requestsFailed = new AtomicLong(0);

    public GestorCarga() {
        this.ctx = new ZContext();
        this.prestamoQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

        // Pool principal de workers para DEV/REN
        this.workerPool = Executors.newFixedThreadPool(NUM_WORKERS, r -> {
            Thread t = new Thread(r, "gc-worker-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });

        // Pool especializado para PRESTAMO (REQ/REP síncrono)
        this.prestamoWorkerPool = Executors.newFixedThreadPool(
                Math.max(4, NUM_WORKERS / 2),
                r -> {
                    Thread t = new Thread(r, "gc-prestamo-worker-" + System.nanoTime());
                    t.setDaemon(true);
                    return t;
                }
        );

        // Executor para reintentos
        this.retryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gc-retry-thread");
            t.setDaemon(true);
            return t;
        });

        System.out.println("[GC] Inicializado con " + NUM_WORKERS + " workers");
    }

    public void iniciarRep(String repBind) {
        rep = ctx.createSocket(ZMQ.REP);
        rep.setReceiveTimeOut(0);
        rep.setLinger(0);
        rep.bind(repBind);
        System.out.println("[GC] REP escuchando en " + repBind);
    }

    public void iniciarPub(String pubBind) {
        pub = ctx.createSocket(ZMQ.PUB);
        pub.setLinger(0);
        pub.bind(pubBind);
        System.out.println("[GC] PUB publicando en " + pubBind);
    }

    public void iniciarRepActorEnqueue(String bind) {
        repActorEnqueue = ctx.createSocket(ZMQ.REP);
        repActorEnqueue.setLinger(0);
        repActorEnqueue.bind(bind);
        System.out.println("[GC] REP (actor-enqueue) escuchando en " + bind);

        String base = "data" + java.io.File.separator + "gc" + java.io.File.separator;
        centralPendingDevoluciones = new DurableQueue(base + "pending_devoluciones.db");
        centralPendingRenovaciones = new DurableQueue(base + "pending_renovaciones.db");
    }

    public void iniciarReqPrestamo(String endpointPrestamo) {
        // Los sockets REQ se crean por worker en el pool de prestamos
        System.out.println("[GC] Configurado endpoint ActorPrestamo: " + endpointPrestamo);
    }

    /**
     * Inicia workers de PRESTAMO que procesan la cola
     */
    private void startPrestamoWorkers(String endpointPrestamo) {
        for (int i = 0; i < Math.max(4, NUM_WORKERS / 2); i++) {
            final int workerId = i;
            prestamoWorkerPool.submit(() -> {
                // Cada worker tiene su propio socket REQ (thread-safe)
                ZMQ.Socket reqPrestamo = ctx.createSocket(ZMQ.REQ);
                reqPrestamo.setLinger(0);
                reqPrestamo.setReceiveTimeOut(5000); // 5s timeout
                reqPrestamo.connect(endpointPrestamo);

                System.out.println("[GC-PrestamoWorker-" + workerId + "] Iniciado");

                while (running) {
                    try {
                        PrestamoRequest request = prestamoQueue.poll(1, TimeUnit.SECONDS);
                        if (request == null) continue;

                        processPrestamoRequest(request, reqPrestamo);

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        System.err.println("[GC-PrestamoWorker-" + workerId + "] Error: " + e.getMessage());
                        requestsFailed.incrementAndGet();
                    }
                }

                reqPrestamo.close();
                System.out.println("[GC-PrestamoWorker-" + workerId + "] Finalizado");
            });
        }
    }

    /**
     * Procesa una solicitud de PRESTAMO de forma síncrona
     */
    private void processPrestamoRequest(PrestamoRequest request, ZMQ.Socket reqPrestamo) {
        try {
            long ts = lamportClock.incrementAndGet();
            String cargaWithTs = request.carga + ";ts=" + ts;

            reqPrestamo.send(cargaWithTs.getBytes(ZMQ.CHARSET), 0);
            byte[] resp = reqPrestamo.recv(0);

            String respuesta = resp != null ? new String(resp, ZMQ.CHARSET) : "ERROR:SinRespuesta";

            if (respuesta.contains("GA_NoDisponible")) {
                // Encolar para reintento
                if (centralPendingPrestamos != null) {
                    centralPendingPrestamos.enqueue(request.carga);
                }
                request.responseFuture.complete("PENDING");
            } else {
                request.responseFuture.complete(respuesta);
            }

            requestsProcessed.incrementAndGet();

        } catch (Exception e) {
            request.responseFuture.completeExceptionally(e);
            requestsFailed.incrementAndGet();
        }
    }

    /**
     * Loop principal multihilo
     */
    public void runLoop(String endpointPrestamo) {
        if (rep == null || pub == null) {
            throw new IllegalStateException("Llama primero a iniciarRep() e iniciarPub().");
        }

        // Inicializar cola central de prestamos
        String base = "data" + java.io.File.separator + "gc" + java.io.File.separator;
        try {
            centralPendingPrestamos = new DurableQueue(base + "pending_prestamos.db");
        } catch (Exception ex) {
            System.err.println("[GC] No se pudo inicializar centralPendingPrestamos: " + ex.getMessage());
        }

        // Iniciar workers de PRESTAMO si hay endpoint configurado
        if (endpointPrestamo != null) {
            startPrestamoWorkers(endpointPrestamo);
        }

        // Iniciar tarea de reintentos
        startRetryTask();

        // Iniciar métricas periódicas
        startMetricsTask();

        // Poller para REP (PS) y REP (Actor-Enqueue)
        ZMQ.Poller poller = ctx.createPoller(2);
        poller.register(rep, ZMQ.Poller.POLLIN);
        int idxActorRep = -1;
        if (repActorEnqueue != null) {
            poller.register(repActorEnqueue, ZMQ.Poller.POLLIN);
            idxActorRep = 1;
        }

        System.out.println("[GC] Entrando en loop principal...");

        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                int rc = poller.poll(100); // 100ms timeout
                if (rc == 0) continue;

                // Procesar requests de PS
                if (poller.pollin(0)) {
                    handlePsRequest();
                }

                // Procesar requests de actores
                if (idxActorRep >= 0 && poller.pollin(idxActorRep)) {
                    handleActorEnqueueRequest();
                }

            } catch (Exception e) {
                if (running) {
                    System.err.println("[GC] Error en loop principal: " + e.getMessage());
                }
            }
        }

        System.out.println("[GC] Loop principal finalizado");
    }

    /**
     * Maneja requests del Productor Simple
     */
    private void handlePsRequest() {
        byte[] req = rep.recv(ZMQ.DONTWAIT);
        if (req == null) return;

        String carga = new String(req, ZMQ.CHARSET);
        long ts = lamportClock.incrementAndGet();

        try {
            if (carga.startsWith("DEVOLUCION") || carga.startsWith("RENOVACION")) {
                // Operaciones asíncronas - procesamiento inmediato
                String topic = carga.startsWith("DEVOLUCION") ? "Devolucion" : "Renovacion";

                // ACK inmediato
                rep.send("ACK".getBytes(ZMQ.CHARSET), 0);

                // Publicar asíncronamente en worker pool
                String cargaWithTs = carga + ";ts=" + ts;
                workerPool.submit(() -> {
                    try {
                        synchronized (pub) {
                            pub.sendMore(topic);
                            pub.send(cargaWithTs);
                        }
                        requestsProcessed.incrementAndGet();
                    } catch (Exception e) {
                        System.err.println("[GC] Error publicando: " + e.getMessage());
                        requestsFailed.incrementAndGet();
                    }
                });

                return;
            }

            if (carga.startsWith("PRESTAMO")) {
                // Operación síncrona - encolar para workers especializados
                PrestamoRequest request = new PrestamoRequest(carga);

                boolean queued = prestamoQueue.offer(request, 2, TimeUnit.SECONDS);

                if (!queued) {
                    rep.send("ERROR:QueueFull".getBytes(ZMQ.CHARSET), 0);
                    requestsFailed.incrementAndGet();
                    return;
                }

                // Esperar respuesta (con timeout)
                try {
                    String respuesta = request.responseFuture.get(10, TimeUnit.SECONDS);
                    rep.send(respuesta.getBytes(ZMQ.CHARSET), 0);
                } catch (TimeoutException e) {
                    rep.send("ERROR:Timeout".getBytes(ZMQ.CHARSET), 0);
                    requestsFailed.incrementAndGet();
                }

                return;
            }

            // Operación desconocida
            rep.send("NACK:OperacionDesconocida".getBytes(ZMQ.CHARSET), 0);

        } catch (Exception e) {
            System.err.println("[GC] Error procesando request PS: " + e.getMessage());
            try {
                rep.send("ERROR:InternalError".getBytes(ZMQ.CHARSET), 0);
            } catch (Exception ex) {
                // Ignorar
            }
            requestsFailed.incrementAndGet();
        }
    }

    /**
     * Maneja requests de actores para encolar operaciones fallidas
     */
    private void handleActorEnqueueRequest() {
        byte[] reqA = repActorEnqueue.recv(ZMQ.DONTWAIT);
        if (reqA == null) return;

        String msg = new String(reqA, ZMQ.CHARSET);

        try {
            if (msg.startsWith("ENQUEUE;")) {
                String payload = msg.substring("ENQUEUE;".length());
                java.util.Map<String, String> kv = Utils.parseKeyValues(payload);
                String type = kv.get("type");
                String cargaOrig = kv.get("carga");

                if (type == null || cargaOrig == null) {
                    repActorEnqueue.send("ERROR:Malformed".getBytes(ZMQ.CHARSET), 0);
                    return;
                }

                if (type.equalsIgnoreCase("Devolucion") && centralPendingDevoluciones != null) {
                    centralPendingDevoluciones.enqueue(cargaOrig);
                    repActorEnqueue.send("ENQUEUED".getBytes(ZMQ.CHARSET), 0);
                } else if (type.equalsIgnoreCase("Renovacion") && centralPendingRenovaciones != null) {
                    centralPendingRenovaciones.enqueue(cargaOrig);
                    repActorEnqueue.send("ENQUEUED".getBytes(ZMQ.CHARSET), 0);
                } else if (type.equalsIgnoreCase("Control")) {
                    synchronized (pub) {
                        pub.sendMore("Failover");
                        pub.send(cargaOrig);
                    }
                    repActorEnqueue.send("ENQUEUED".getBytes(ZMQ.CHARSET), 0);
                } else {
                    repActorEnqueue.send("ERROR:UnknownType".getBytes(ZMQ.CHARSET), 0);
                }
            } else {
                repActorEnqueue.send("ERROR:Unsupported".getBytes(ZMQ.CHARSET), 0);
            }
        } catch (Exception e) {
            System.err.println("[GC] Error procesando actor-enqueue: " + e.getMessage());
            try {
                repActorEnqueue.send("ERROR:InternalError".getBytes(ZMQ.CHARSET), 0);
            } catch (Exception ex) {
                // Ignorar
            }
        }
    }

    /**
     * Tarea de reintentos periódica
     */
    private void startRetryTask() {
        retryExecutor.scheduleWithFixedDelay(() -> {
            try {
                // Procesar cola durable de prestamos
                if (centralPendingPrestamos != null) {
                    centralPendingPrestamos.processAll(item -> {
                        try {
                            PrestamoRequest request = new PrestamoRequest(item);
                            boolean queued = prestamoQueue.offer(request, 1, TimeUnit.SECONDS);

                            if (!queued) {
                                return false; // Mantener en cola
                            }

                            String respuesta = request.responseFuture.get(5, TimeUnit.SECONDS);
                            if (respuesta.contains("GA_NoDisponible")) {
                                return false; // Mantener en cola
                            }

                            System.out.println("[GC-retry] PRESTAMO procesado: " + respuesta);
                            return true; // Remover de cola

                        } catch (Exception ex) {
                            return false; // Mantener en cola
                        }
                    });
                }

                // Procesar colas de DEV/REN
                if (centralPendingDevoluciones != null) {
                    centralPendingDevoluciones.processAll(item -> {
                        try {
                            long ts = lamportClock.incrementAndGet();
                            String cargaWithTs = item + ";ts=" + ts;
                            synchronized (pub) {
                                pub.sendMore("Devolucion");
                                pub.send(cargaWithTs);
                            }
                            return true;
                        } catch (Exception ex) {
                            return false;
                        }
                    });
                }

                if (centralPendingRenovaciones != null) {
                    centralPendingRenovaciones.processAll(item -> {
                        try {
                            long ts = lamportClock.incrementAndGet();
                            String cargaWithTs = item + ";ts=" + ts;
                            synchronized (pub) {
                                pub.sendMore("Renovacion");
                                pub.send(cargaWithTs);
                            }
                            return true;
                        } catch (Exception ex) {
                            return false;
                        }
                    });
                }

            } catch (Exception ex) {
                System.err.println("[GC-retry] Error durante reintento: " + ex.getMessage());
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * Tarea de métricas periódica
     */
    private void startMetricsTask() {
        retryExecutor.scheduleWithFixedDelay(() -> {
            long processed = requestsProcessed.get();
            long failed = requestsFailed.get();
            int queueSize = prestamoQueue.size();

            System.out.printf(
                    "[GC-Metrics] Procesados: %d | Fallidos: %d | Cola: %d | LamportClock: %d%n",
                    processed, failed, queueSize, lamportClock.get()
            );
        }, 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        System.out.println("[GC] Cerrando GestorCarga...");
        running = false;

        // Shutdown thread pools
        workerPool.shutdown();
        prestamoWorkerPool.shutdown();
        retryExecutor.shutdown();

        try {
            if (!workerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                workerPool.shutdownNow();
            }
            if (!prestamoWorkerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                prestamoWorkerPool.shutdownNow();
            }
            if (!retryExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                retryExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            workerPool.shutdownNow();
            prestamoWorkerPool.shutdownNow();
            retryExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Cerrar sockets
        if (rep != null) rep.close();
        if (pub != null) pub.close();
        if (repActorEnqueue != null) repActorEnqueue.close();

        // Cerrar contexto
        ctx.close();

        System.out.println("[GC] Cerrado completamente.");
        System.out.printf("[GC] Stats finales - Procesados: %d | Fallidos: %d%n",
                requestsProcessed.get(), requestsFailed.get());
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
            gc.runLoop(endpointActorPrestamo);
        }
    }

    /**
     * Clase para encapsular request de PRESTAMO con Future para respuesta
     */
    private static class PrestamoRequest {
        final String carga;
        final CompletableFuture<String> responseFuture;

        PrestamoRequest(String carga) {
            this.carga = carga;
            this.responseFuture = new CompletableFuture<>();
        }
    }
}