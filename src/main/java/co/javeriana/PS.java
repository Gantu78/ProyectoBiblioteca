package co.javeriana;

import java.io.BufferedReader;
import java.io.*;
import java.util.*;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

public class PS {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: <endpoint_gc_req> <archivo_entrada>");
            System.out.println("Ejemplo: tcp://192.168.1.10:5555 entrada.txt");
            return;
        }

        final String endpoint = args[0];
        final String archivoEntrada = args[1];

        // Permite pasar ruta absoluta/relativa o solo nombre (lo busca en resources)
        File archivo = new File(archivoEntrada);
        if (!archivo.exists()) {
            archivo = new File("src/main/resources/" + archivoEntrada);
        }
        if (!archivo.exists()) {
            System.err.println("[ERROR] No se encontró el archivo: " + archivoEntrada);
            return;
        }

        // Carga y filtra líneas válidas
        List<String> solicitudes = loadRequests(archivo.getPath());
        if (solicitudes.isEmpty()) {
            System.out.println("[INFO] No hay líneas válidas para enviar.");
            return;
        }

        try (ZContext context = new ZContext()) {
            System.out.println("Conectando a: " + endpoint);
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.setLinger(0);
            socket.connect(endpoint);

            int n = 1;
            for (String linea : solicitudes) {
                socket.send(linea.getBytes(ZMQ.CHARSET), 0);
                System.out.println("Enviando línea " + n + ": " + linea);

                byte[] reply = socket.recv(0);
                String resp = reply != null ? new String(reply, ZMQ.CHARSET) : "<sin respuesta>";
                System.out.println("Recibido: [" + resp + "]");
                n++;
            }
        }
    }

    /** Lee el archivo y devuelve solo líneas válidas (DEVOLUCION/RENOVACION), ignorando vacías y comentarios. */
    private static List<String> loadRequests(String path) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path, ZMQ.CHARSET))) {
            String s;
            while ((s = br.readLine()) != null) {
                s = s.trim();
                if (s.isEmpty() || s.startsWith("#")) continue;
                if (!s.startsWith("DEVOLUCION") && !s.startsWith("RENOVACION")) {
                    System.err.println("[WARN] Línea ignorada (operación desconocida): " + s);
                    continue;
                }
                lines.add(s);
            }
        }
        return lines;
    }
}