package grpc;

import grpc.generated.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Map;

public class NodoGRPC {

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

    private long tick() {
        synchronized (lock) { return ++lamportClock; }
    }

    private long update(long receivedTs) {
        synchronized (lock) {
            lamportClock = Math.max(lamportClock, receivedTs) + 1;
            return lamportClock;
        }
    }

    private long getClock() {
        synchronized (lock) { return lamportClock; }
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
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.shutdown()));
        return server;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) { System.exit(1); }
        int nodeId = Integer.parseInt(args[0]);
        NodoGRPC nodo = new NodoGRPC(nodeId);
        Server server = nodo.startServer();
        System.out.printf("[Nodo %d] Esperando mensajes...%n", nodeId);
        server.awaitTermination();
    }
}