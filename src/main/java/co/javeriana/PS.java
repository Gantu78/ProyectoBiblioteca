package co.javeriana;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class PS {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.println("Uso: <endpoint_gc_req> <puerto_http>");
            System.out.println("Ejemplo: tcp://192.168.1.10:5555 8081");
            return;
        }

        final String endpoint = args[0];
        final int puerto = Integer.parseInt(args[1]);

        System.out.println("[PS] Conectando a GC en " + endpoint);
        System.out.println("[PS] Iniciando HTTP en puerto " + puerto);

        ZContext context = new ZContext();
        ZMQ.Socket socket = context.createSocket(SocketType.REQ);
        socket.setLinger(0);
        socket.connect(endpoint);

        // Servidor HTTP local usando el puerto pasado por parámetro
        HttpServer server = HttpServer.create(new InetSocketAddress(puerto), 0);
        System.out.println("[PS] HTTP server escuchando en http://localhost:" + puerto + "/send");

        server.createContext("/send", (HttpExchange exchange) -> {
            try {
                if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }

                String body = new String(exchange.getRequestBody().readAllBytes()).trim();
                System.out.println("[PS] Recibido desde Locust: " + body);

                // Enviar al GC
                socket.send(body.getBytes(ZMQ.CHARSET), 0);

                byte[] reply = socket.recv(0);
                String resp = reply != null ? new String(reply, ZMQ.CHARSET) : "<sin respuesta>";

                System.out.println("[PS] Respuesta GC: " + resp);

                byte[] response = resp.getBytes();
                exchange.sendResponseHeaders(200, response.length);

                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response);
                }

            } catch (Exception e) {
                e.printStackTrace();
                String err = "ERROR: " + e.getMessage();
                exchange.sendResponseHeaders(500, err.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(err.getBytes());
                }
            }
        });

        server.start();
    }
}
