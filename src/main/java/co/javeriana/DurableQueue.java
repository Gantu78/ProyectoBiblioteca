package co.javeriana;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Cola persistente muy simple: cada elemento es una línea en el fichero.
 * - enqueue escribe (append) de forma sincronizada.
 * - processAll aplica un procesador a cada elemento; si el procesador devuelve
 *   true, el elemento se elimina de la cola en disco (se reescribe el fichero).
 */
public class DurableQueue {
    private final Path file;

    public DurableQueue(String filePath) {
        this.file = Path.of(filePath);
        try {
            if (file.getParent() != null) Files.createDirectories(file.getParent());
            if (!Files.exists(file)) Files.createFile(file);
        } catch (IOException e) {
            System.err.println("[DurableQueue] No se pudo inicializar cola en " + filePath + ": " + e.getMessage());
        }
    }

    public synchronized void enqueue(String item) {
        try (Writer w = new OutputStreamWriter(new FileOutputStream(file.toFile(), true), StandardCharsets.UTF_8)) {
            w.write(item.replaceAll("\r?\n", " ") + System.lineSeparator());
            w.flush();
        } catch (IOException e) {
            System.err.println("[DurableQueue] Error en enqueue: " + e.getMessage());
        }
    }

    public synchronized List<String> snapshot() {
        try {
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            List<String> cleaned = new ArrayList<>();
            for (String l : lines) if (l != null && !l.trim().isEmpty()) cleaned.add(l.trim());
            return cleaned;
        } catch (IOException e) {
            System.err.println("[DurableQueue] Error leyendo snapshot: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Procesa todos los elementos actuales aplicando el processor. Si devuelve true,
     * el elemento se elimina de la cola. Al final reescribe el fichero con los
     * elementos restantes de forma atómica.
     */
    public synchronized void processAll(java.util.function.Function<String, Boolean> processor) {
        List<String> items = snapshot();
        if (items.isEmpty()) return;

        List<String> remaining = new ArrayList<>();
        for (String it : items) {
            boolean ok = false;
            try {
                ok = Boolean.TRUE.equals(processor.apply(it));
            } catch (Exception ex) {
                System.err.println("[DurableQueue] Processor raised: " + ex.getMessage());
                ok = false;
            }
            if (!ok) remaining.add(it);
        }

        // rewrite file atomically
        try {
            Path tmp = file.resolveSibling(file.getFileName().toString() + ".tmp");
            try (Writer w = new OutputStreamWriter(new FileOutputStream(tmp.toFile()), StandardCharsets.UTF_8)) {
                for (String r : remaining) {
                    w.write(r + System.lineSeparator());
                }
                w.flush();
            }
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception e) {
            try {
                // fallback non-atomic
                Path tmp = file.resolveSibling(file.getFileName().toString() + ".tmp");
                try (Writer w = new OutputStreamWriter(new FileOutputStream(tmp.toFile()), StandardCharsets.UTF_8)) {
                    for (String r : remaining) w.write(r + System.lineSeparator());
                    w.flush();
                }
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            } catch (Exception ex) {
                System.err.println("[DurableQueue] Error reescribiendo cola: " + ex.getMessage());
            }
        }
    }
}
