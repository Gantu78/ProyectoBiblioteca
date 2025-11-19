package co.javeriana;

/**
 * ReplicaManager mantiene referencias a un GestorAlmacenamiento primario y uno réplica.
 * Permite consultar el GA activo y forzar una conmutación a la réplica cuando la primaria falla.
 *
 * - `getActivo()` devuelve una instancia de `GestorAlmacenamientoConReplica` que aplica
 *   operaciones a la primaria y replica según el modo actual.
 * - `primariaActiva()` indica si la primaria está siendo utilizada.
 * - `conmutarAReplica()` fuerza la conmutación: a partir de ese momento las operaciones
 *   se realizarán únicamente sobre la réplica.
 */
public class ReplicaManager {
    private final GestorAlmacenamiento primaria;
    private final GestorAlmacenamiento replica;
    private final GestorAlmacenamientoConReplica compuesto;
    private volatile boolean primariaActiva = true;

    public ReplicaManager(GestorAlmacenamiento primaria, GestorAlmacenamiento replica) {
        this.primaria = primaria;
        this.replica = replica;
        this.compuesto = new GestorAlmacenamientoConReplica(primaria, replica, this);
        // Intentar sincronizar réplica desde primaria al arrancar si es necesario
        try {
            boolean synced = syncReplicaFromPrimary();
            if (synced) System.err.println("[ReplicaManager] Replica inicial sincronizada desde primaria.");
        } catch (Exception ex) {
            System.err.println("[ReplicaManager] No se pudo sincronizar réplica al arrancar: " + ex.getMessage());
        }
    }

    /** Devuelve el gestor compuesto que maneja escrituras a primaria y réplica según el estado. */
    public GestorAlmacenamientoConReplica getActivo() {
        return compuesto;
    }

    public boolean primariaActiva() {
        return primariaActiva;
    }

    /**
     * Forzar conmutación a réplica. Las llamadas posteriores usarán la réplica.
     * Se deja la réplica como el destino único.
     */
    public void conmutarAReplica() {
        System.err.println("[ReplicaManager] Iniciando conmutación a réplica (failover)");
        try {
            boolean ok = syncReplicaFromPrimary();
            if (!ok) {
                System.err.println("[ReplicaManager] Advertencia: sincronización a réplica falló, procediendo de todos modos.");
            } else {
                System.err.println("[ReplicaManager] Sincronización a réplica completada.");
            }
        } catch (Exception ex) {
            System.err.println("[ReplicaManager] Error durante syncReplicaFromPrimary: " + ex.getMessage());
        }
        primariaActiva = false;
        compuesto.setPrimariaActiva(false);
    }

    /**
     * Sincroniza los ficheros básicos de datos desde `data/primaria` a `data/replica`.
     * Intenta copiar `libros.db` y `prestamos.db` de forma atómica usando ficheros temporales
     * y bloqueos sencillos para evitar corrupciones por concurrencia con otros procesos.
     *
     * Retorna true si al menos los ficheros presentes fueron copiados correctamente.
     */
    public boolean syncReplicaFromPrimary() {
        java.nio.file.Path primariaDir = java.nio.file.Paths.get("data", "primaria");
        java.nio.file.Path replicaDir = java.nio.file.Paths.get("data", "replica");
        try {
            java.nio.file.Files.createDirectories(replicaDir);
        } catch (Exception e) {
            System.err.println("[ReplicaManager] No se pudo crear directorio de réplica: " + e.getMessage());
            return false;
        }

        String[] files = new String[]{"libros.db", "prestamos.db"};
        boolean anyOk = false;

        for (String fname : files) {
            java.nio.file.Path src = primariaDir.resolve(fname);
            java.nio.file.Path dst = replicaDir.resolve(fname);
            if (!java.nio.file.Files.exists(src)) continue; // nada que copiar

            java.nio.file.Path srcLock = primariaDir.resolve(fname + ".lock");
            java.nio.file.Path dstLock = replicaDir.resolve(fname + ".lock");

            // Acquire locks in deterministic order (by path string) to avoid deadlocks
            java.nio.file.Path firstLock = srcLock.toString().compareTo(dstLock.toString()) <= 0 ? srcLock : dstLock;
            java.nio.file.Path secondLock = firstLock == srcLock ? dstLock : srcLock;

            try (java.io.RandomAccessFile raf1 = new java.io.RandomAccessFile(firstLock.toFile(), "rw");
                 java.nio.channels.FileChannel ch1 = raf1.getChannel();
                 java.nio.channels.FileLock l1 = ch1.lock()) {

                try (java.io.RandomAccessFile raf2 = new java.io.RandomAccessFile(secondLock.toFile(), "rw");
                     java.nio.channels.FileChannel ch2 = raf2.getChannel();
                     java.nio.channels.FileLock l2 = ch2.lock()) {

                    // perform copy to temp then atomic move
                    java.nio.file.Path tmp = dst.resolveSibling(dst.getFileName().toString() + ".tmp");
                    try {
                        java.nio.file.Files.copy(src, tmp, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                        try {
                            java.nio.file.Files.move(tmp, dst, java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                        } catch (Exception moveEx) {
                            // fallback to non-atomic move
                            java.nio.file.Files.move(tmp, dst, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                        }
                        anyOk = true;
                        System.err.println("[ReplicaManager] Copiado " + src.toString() + " -> " + dst.toString());
                    } catch (Exception copyEx) {
                        System.err.println("[ReplicaManager] Error copiando " + src.toString() + ": " + copyEx.getMessage());
                        try { if (java.nio.file.Files.exists(tmp)) java.nio.file.Files.delete(tmp); } catch (Exception ignore) {}
                    }

                }
            } catch (Exception lockEx) {
                System.err.println("[ReplicaManager] Error adquiriendo locks para " + fname + ": " + lockEx.getMessage());
            }
        }

        return anyOk;
    }

    /** NOTA: Por ahora no implementamos failback automático. */
}
