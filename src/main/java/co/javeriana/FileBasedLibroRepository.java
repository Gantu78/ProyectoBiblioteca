package co.javeriana;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileBasedLibroRepository implements LibroRepository {
    private final File file;
    private final Map<String, Libro> libros;
    private final Gson gson = new Gson();

    public FileBasedLibroRepository(String path) {
        this.file = new File(path);
        this.libros = loadFromDisk();
    }

    private Map<String, Libro> loadFromDisk() {
        if (!file.exists()) {
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();
            return new ConcurrentHashMap<>();
        }
        try (InputStreamReader r = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
            Type type = new TypeToken<Map<String, Libro>>(){}.getType();
            Map<String, Libro> map = gson.fromJson(r, type);
            if (map != null) return new ConcurrentHashMap<>(map);
        } catch (Exception e) {
            System.err.println("[FileBasedLibroRepository] Error cargando desde disco: " + e.getMessage());
            // backup corrupted/non-json file so next run can recreate
            try {
                File bak = new File(file.getAbsolutePath() + ".bak");
                java.nio.file.Files.move(file.toPath(), bak.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                System.err.println("[FileBasedLibroRepository] Archivo corrupto movido a: " + bak.getAbsolutePath());
            } catch (Exception ex) {
                System.err.println("[FileBasedLibroRepository] No se pudo mover archivo corrupto: " + ex.getMessage());
            }
        }
        return new ConcurrentHashMap<>();
    }

    private synchronized void persist() {
        File lockFile = new File(file.getAbsolutePath() + ".lock");
        File tmp = new File(file.getAbsolutePath() + ".tmp");
        // Use a file lock on a dedicated lock file to coordinate across processes
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(lockFile, "rw");
             java.nio.channels.FileChannel channel = raf.getChannel();
             java.nio.channels.FileLock lock = channel.lock()) {

            try (OutputStreamWriter w = new OutputStreamWriter(new FileOutputStream(tmp), StandardCharsets.UTF_8)) {
                gson.toJson(libros, w);
                w.flush();
                w.close();
            }
            try {
                java.nio.file.Files.move(tmp.toPath(), file.toPath(), java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            } catch (Exception ex) {
                if (!tmp.renameTo(file)) {
                    System.err.println("[FileBasedLibroRepository] Error renombrando temp file: " + ex.getMessage());
                }
            }

        } catch (IOException e) {
            System.err.println("[FileBasedLibroRepository] Error persistiendo: " + e.getMessage());
            if (tmp.exists()) tmp.delete();
        }
    }

    @Override
    public Libro findByCodigo(String codigo) {
        return libros.get(codigo);
    }

    @Override
    public void save(Libro libro) {
        libros.put(libro.getCodigo(), libro);
        persist();
    }
}
