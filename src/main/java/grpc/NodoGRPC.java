package grpc;

import grpc.generated.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class NodoGRPC {

    // Configuración de los tres nodos
    private static final Map<Integer, Integer> PUERTOS = Map.of(
            1, 6000,
            2, 6001,
            3, 6002
    );

    private final int nodeId;
    private long lamportClock = 0;
    private final Object lock = new Object();

    public NodoGRPC(int nodeId) {
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

    // Servidor gRPC
    private class NodoServiceImpl extends NodoServiceGrpc.NodoServiceImplBase {
        @Override
        public void enviar(Mensaje request, StreamObserver<Respuesta> responseObserver) {
            long newClock = update(request.getTimestamp());

            System.out.printf("[Nodo %d] ← %s | msg='%s' | Lamport=%d%n",
                    nodeId,
                    request.getSender(),
                    request.getMessage(),
                    newClock);

            Respuesta resp = Respuesta.newBuilder()
                    .setResponder("Nodo" + nodeId)
                    .setTimestamp(newClock)
                    .build();

            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }
    }

    public Server startServer() throws IOException {
        int port = PUERTOS.get(nodeId);
        Server server = ServerBuilder
                .forPort(port)
                .addService(new NodoServiceImpl())
                .build()
                .start();

        System.out.printf("[Nodo %d] Servidor gRPC activo en puerto %d%n", nodeId, port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n[Nodo " + nodeId + "] Apagando servidor gRPC...");
            server.shutdown();
        }));

        return server;
    }

    //Cliente gRPC
    private void sendTo(int targetId, String message) {
        int port = PUERTOS.get(targetId);
        long ts  = tick();

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", port)
                .usePlaintext()
                .build();

        try {
            NodoServiceGrpc.NodoServiceBlockingStub stub =
                    NodoServiceGrpc.newBlockingStub(channel);

            Mensaje msg = Mensaje.newBuilder()
                    .setSender("Nodo" + nodeId)
                    .setTimestamp(ts)
                    .setMessage(message)
                    .build();

            Respuesta resp = stub.enviar(msg);
            update(resp.getTimestamp());

        } finally {
            // Cerrar el canal al terminar
            channel.shutdown();
            try {
                channel.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    //20 Intercambios

    public void runExchanges() {
        List<Integer> targets = PUERTOS.keySet().stream()
                .filter(id -> id != nodeId)
                .sorted()
                .collect(Collectors.toList());

        System.out.println("\n[Nodo " + nodeId + "] ── 20 INTERCAMBIOS gRPC ──");

        for (int i = 0; i < 20; i++) {
            int target = targets.get(i % targets.size());
            sendTo(target, "intercambio_" + (i + 1));
            System.out.printf("  [%2d/20] → Nodo %d | Lamport=%d%n",
                    i + 1, target, getClock());
        }
    }

    // 100 Envíos para medir latencia

    public List<Double> measureLatency() {
        List<Integer> targets = PUERTOS.keySet().stream()
                .filter(id -> id != nodeId)
                .sorted()
                .collect(Collectors.toList());
        int target = targets.get(0);

        List<Double> latencies = new ArrayList<>();
        System.out.println("\n[Nodo " + nodeId + "] ── 100 ENVÍOS PARA LATENCIA → Nodo " + target + " ──");

        for (int i = 0; i < 100; i++) {
            long start = System.nanoTime();
            sendTo(target, "latencia_" + (i + 1));
            double ms  = (System.nanoTime() - start) / 1_000_000.0;
            latencies.add(ms);
        }

        double avg = latencies.stream().mapToDouble(d -> d).average().orElse(0);
        System.out.printf("  Latencia promedio: %.4f ms%n", avg);
        return latencies;
    }

    //Guardar CSV

    public void saveCSV(List<Double> latencies) throws IOException {
        String filename = "resultados_nodo" + nodeId + "_grpc.csv";
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
            System.out.println("Uso: java -cp <jar> grpc.NodoGRPC <1|2|3>");
            System.exit(1);
        }

        int nodeId = Integer.parseInt(args[0]);
        NodoGRPC nodo = new NodoGRPC(nodeId);
        Server server = nodo.startServer();

        if (nodeId == 1) {
            System.out.println("[Nodo 1] Esperando que los nodos 2 y 3 estén listos... (3s)");
            Thread.sleep(3000);

            nodo.runExchanges();
            List<Double> latencies = nodo.measureLatency();
            nodo.saveCSV(latencies);

        } else {
            System.out.printf("[Nodo %d] Listo. Esperando mensajes del Nodo 1...%n", nodeId);
            server.awaitTermination();
        }
    }
}