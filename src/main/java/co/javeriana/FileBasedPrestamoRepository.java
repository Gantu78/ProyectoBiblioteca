package co.javeriana;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileBasedPrestamoRepository implements PrestamoRepository {
    private final File file;
    private final Map<String, Prestamo> prestamos;
    private final Gson gson = new Gson();

    public FileBasedPrestamoRepository(String path) {
        this.file = new File(path);
        this.prestamos = loadFromDisk();
    }

    private Map<String, Prestamo> loadFromDisk() {
        if (!file.exists()) {
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            return new ConcurrentHashMap<>();
        }
        try (InputStreamReader r = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
            Type type = new TypeToken<Map<String, Prestamo>>(){}.getType();
            Map<String, Prestamo> map = gson.fromJson(r, type);
            if (map != null) return new ConcurrentHashMap<>(map);
        } catch (Exception e) {
            System.err.println("[FileBasedPrestamoRepository] Error cargando desde disco: " + e.getMessage());
            try {
                File bak = new File(file.getAbsolutePath() + ".bak");
                java.nio.file.Files.move(file.toPath(), bak.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                System.err.println("[FileBasedPrestamoRepository] Archivo corrupto movido a: " + bak.getAbsolutePath());
            } catch (Exception ex) {
                System.err.println("[FileBasedPrestamoRepository] No se pudo mover archivo corrupto: " + ex.getMessage());
            }
        }
        return new ConcurrentHashMap<>();
    }

    private synchronized void persist() {
        File lockFile = new File(file.getAbsolutePath() + ".lock");
        File tmp = new File(file.getAbsolutePath() + ".tmp");
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(lockFile, "rw");
             java.nio.channels.FileChannel channel = raf.getChannel();
             java.nio.channels.FileLock lock = channel.lock()) {

            try (OutputStreamWriter w = new OutputStreamWriter(new FileOutputStream(tmp), StandardCharsets.UTF_8)) {
                gson.toJson(prestamos, w);
                w.flush();
                w.close();
            }
            try {
                java.nio.file.Files.move(tmp.toPath(), file.toPath(), java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            } catch (Exception ex) {
                if (!tmp.renameTo(file)) {
                    System.err.println("[FileBasedPrestamoRepository] Error renombrando temp file: " + ex.getMessage());
                }
            }

        } catch (IOException e) {
            System.err.println("[FileBasedPrestamoRepository] Error persistiendo: " + e.getMessage());
            if (tmp.exists()) tmp.delete();
        }
    }

    @Override
    public Prestamo findById(String id) {
        return prestamos.get(id);
    }

    @Override
    public void save(Prestamo p) {
        prestamos.put(p.getId(), p);
        persist();
    }
}
