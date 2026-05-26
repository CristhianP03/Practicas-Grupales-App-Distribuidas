package tcp;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.*;
import java.net.*;
import java.util.Map;

public class NodoTCP {

    private static final Map<Integer, Integer> PUERTOS = Map.of(
            1, 5000,
            2, 5001,
            3, 5002
    );

    private final int nodeId;
    private final Gson gson = new Gson();

    public NodoTCP(int nodeId) {
        this.nodeId = nodeId;
    }

    public void startServer() {
        Thread t = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(PUERTOS.get(nodeId))) {
                serverSocket.setReuseAddress(true);
                System.out.printf("[Nodo %d] Servidor TCP en puerto %d%n",
                        nodeId, PUERTOS.get(nodeId));
                while (true) {
                    Socket client = serverSocket.accept();
                    new Thread(() -> handleConnection(client)).start();
                }
            } catch (IOException e) {
                System.err.println("[Nodo " + nodeId + "] Error: " + e.getMessage());
            }
        });
        t.setDaemon(true);
        t.start();
    }

    private void handleConnection(Socket socket) {
        try (socket;
             BufferedReader in  = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter    out = new PrintWriter(socket.getOutputStream(), true)) {

            String raw = in.readLine();
            if (raw == null) return;

            JsonObject msg = gson.fromJson(raw, JsonObject.class);
            System.out.printf("[Nodo %d] Recibido de %s: '%s'%n",
                    nodeId,
                    msg.get("sender").getAsString(),
                    msg.get("message").getAsString());

            JsonObject resp = new JsonObject();
            resp.addProperty("status", "ok");
            out.println(resp.toString());

        } catch (IOException e) {
            System.err.println("[Nodo " + nodeId + "] Error en conexión: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) { System.exit(1); }
        int nodeId = Integer.parseInt(args[0]);
        NodoTCP nodo = new NodoTCP(nodeId);
        nodo.startServer();
        Thread.currentThread().join();
    }
}