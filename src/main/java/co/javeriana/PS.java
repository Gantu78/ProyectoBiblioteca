package co.javeriana;

import java.io.BufferedReader;

import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZContext;

public class PS
{
    public static void main(String[] args) throws Exception
    {
        if(args.length < 2) {
            System.out.println("Uso: <endpoint_gc_req> <archivo_entrada>");
            System.out.println("Ejemplo: tcp://192.168.1.10:5555 entrada.txt");
            return;
        }
        final String endpoint = args[0];
        final String archivoEntrada = args[1];
        try (ZContext context = new ZContext()) {
            File archivo = new File("src/main/resources/" + archivoEntrada);
            BufferedReader br = new BufferedReader(new FileReader(archivo));

            //  Socket to talk to server
            System.out.println("Conectando a: " + endpoint);
            ZMQ.Socket socket = context.createSocket(SocketType.REQ);
            socket.connect(endpoint);

            String linea;
            int n=0;
            while ((linea = br.readLine()) != null) {
                String slinea = linea.trim();
                if (slinea.isEmpty() || slinea.startsWith("#")) {
                    continue;
                }
                socket.send(slinea.getBytes(ZMQ.CHARSET), 0);
                System.out.println("Enviando linea " + n + ": " + slinea);
                byte[] reply = socket.recv(0);
                System.out.println(
                    "Recibido " + ": [" + new String(reply, ZMQ.CHARSET) + "]"
                );
                n++;
            }
            br.close();
        }
    }
}