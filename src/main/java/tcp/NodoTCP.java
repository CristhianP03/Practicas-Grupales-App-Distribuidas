package tcp;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.stream.Collectors;

public class NodoTCP {

    // Configuración de los tres nodos
    private static final Map<Integer, Integer> PUERTOS = Map.of(
            1, 5000,
            2, 5001,
            3, 5002
    );

    private final int nodeId;
    private long lamportClock = 0;
    private final Object lock = new Object();   // Protege el reloj (multi-hilo)
    private final Gson gson = new Gson();

    public NodoTCP(int nodeId) {
        this.nodeId = nodeId;
    }

    // Reloj de Lamport

    private long tick() {
        synchronized (lock) {
            return ++lamportClock;
        }
    }

    private long update(long receivedTs) {
        synchronized (lock) {
            lamportClock = Math.max(lamportClock, receivedTs) + 1;
            return lamportClock;
        }
    }

    private long getClock() {
        synchronized (lock) {
            return lamportClock;
        }
    }

    // Servidor (hilo en segundo plano)

    public void startServer() {
        Thread serverThread = new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(PUERTOS.get(nodeId))) {
                serverSocket.setReuseAddress(true);
                System.out.printf("[Nodo %d] Servidor TCP activo en puerto %d%n",
                        nodeId, PUERTOS.get(nodeId));
                while (true) {
                    Socket client = serverSocket.accept();
                    // Cada conexión en su propio hilo
                    new Thread(() -> handleConnection(client)).start();
                }
            } catch (IOException e) {
                System.err.println("[Nodo " + nodeId + "] Error en servidor: " + e.getMessage());
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();
    }

    private void handleConnection(Socket socket) {
        try (socket;
             BufferedReader in  = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter    out = new PrintWriter(socket.getOutputStream(), true)) {

            String raw = in.readLine();
            if (raw == null) return;

            JsonObject msg = gson.fromJson(raw, JsonObject.class);
            long receivedTs = msg.get("timestamp").getAsLong();
            long newClock   = update(receivedTs);

            System.out.printf("[Nodo %d] ← %s | msg='%s' | Lamport=%d%n",
                    nodeId,
                    msg.get("sender").getAsString(),
                    msg.get("message").getAsString(),
                    newClock);

            // Respuesta con timestamp actualizado
            JsonObject resp = new JsonObject();
            resp.addProperty("responder", nodeId);
            resp.addProperty("timestamp", newClock);
            out.println(resp.toString());

        } catch (IOException e) {
            System.err.println("[Nodo " + nodeId + "] Error manejando conexión: " + e.getMessage());
        }
    }

    // Cliente

    private void sendTo(int targetId, String message) throws IOException {
        int port = PUERTOS.get(targetId);
        long ts  = tick();

        JsonObject msg = new JsonObject();
        msg.addProperty("sender",    "Nodo" + nodeId);
        msg.addProperty("timestamp", ts);
        msg.addProperty("message",   message);

        try (Socket socket = new Socket("localhost", port);
             PrintWriter    out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in  = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println(msg.toString());

            String raw = in.readLine();
            if (raw != null) {
                JsonObject resp = gson.fromJson(raw, JsonObject.class);
                update(resp.get("timestamp").getAsLong());
            }
        }
    }

    // 20 Intercambios

    public void runExchanges() throws IOException {
        List<Integer> targets = PUERTOS.keySet().stream()
                .filter(id -> id != nodeId)
                .sorted()
                .collect(Collectors.toList());

        System.out.println("\n[Nodo " + nodeId + "] ── 20 INTERCAMBIOS ──");

        for (int i = 0; i < 20; i++) {
            int target = targets.get(i % targets.size());
            sendTo(target, "intercambio_" + (i + 1));
            System.out.printf("  [%2d/20] → Nodo %d | Lamport=%d%n",
                    i + 1, target, getClock());
        }
    }

    // 100 Envíos para medir latencia

    public List<Double> measureLatency() throws IOException {
        List<Integer> targets = PUERTOS.keySet().stream()
                .filter(id -> id != nodeId)
                .sorted()
                .collect(Collectors.toList());
        int target = targets.get(0);

        List<Double> latencies = new ArrayList<>();
        System.out.println("\n[Nodo " + nodeId + "] ── 100 ENVÍOS PARA LATENCIA → Nodo " + target + " ──");

        for (int i = 0; i < 100; i++) {
            long start   = System.nanoTime();
            sendTo(target, "latencia_" + (i + 1));
            double ms    = (System.nanoTime() - start) / 1_000_000.0;   // ns → ms
            latencies.add(ms);
        }

        double avg = latencies.stream().mapToDouble(d -> d).average().orElse(0);
        System.out.printf("  Latencia promedio: %.4f ms%n", avg);
        return latencies;
    }

    // Guardar CSV

    public void saveCSV(List<Double> latencies) throws IOException {
        String filename = "resultados_nodo" + nodeId + "_tcp.csv";
        try (PrintWriter pw = new PrintWriter(new FileWriter(filename))) {
            pw.println("envio,latencia_ms");
            for (int i = 0; i < latencies.size(); i++) {
                pw.printf("%d,%.4f%n", i + 1, latencies.get(i));
            }
            double avg = latencies.stream().mapToDouble(d -> d).average().orElse(0);
            pw.printf("PROMEDIO,%.4f%n", avg);
        }
        System.out.println("\n  CSV guardado: " + filename);
    }

    // Main

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || !args[0].matches("[123]")) {
            System.out.println("Uso: java -cp <jar> tcp.NodoTCP <1|2|3>");
            System.exit(1);
        }

        int nodeId = Integer.parseInt(args[0]);
        NodoTCP nodo = new NodoTCP(nodeId);
        nodo.startServer();

        if (nodeId == 1) {
            System.out.println("[Nodo 1] Esperando que los nodos 2 y 3 estén listos... (3s)");
            Thread.sleep(3000);

            nodo.runExchanges();
            List<Double> latencies = nodo.measureLatency();
            nodo.saveCSV(latencies);

        } else {
            System.out.printf("[Nodo %d] Listo. Esperando mensajes del Nodo 1...%n", nodeId);
            Thread.currentThread().join();
        }
    }
}