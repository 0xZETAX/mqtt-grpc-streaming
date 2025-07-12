// Java - MQTT & gRPC Streaming Examples
// Dependencies: org.eclipse.paho:org.eclipse.paho.client.mqttv3, io.grpc:grpc-all

import org.eclipse.paho.client.mqttv3.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.time.Instant;
import java.util.Random;
import com.google.gson.Gson;

public class StreamingExamples {
    
    // ============ MQTT EXAMPLES ============
    
    static class MQTTPublisher {
        private MqttClient client;
        private Gson gson = new Gson();
        private Random random = new Random();
        
        public MQTTPublisher(String brokerUrl) throws MqttException {
            client = new MqttClient(brokerUrl, "JavaPublisher");
            client.connect();
        }
        
        public void publishSensorData() {
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            
            executor.scheduleAtFixedRate(() -> {
                try {
                    SensorData data = new SensorData(
                        Instant.now().toString(),
                        random.nextDouble() * 100,
                        "temperature"
                    );
                    
                    String message = gson.toJson(data);
                    MqttMessage mqttMessage = new MqttMessage(message.getBytes());
                    mqttMessage.setQos(1);
                    
                    client.publish("sensors/temperature", mqttMessage);
                    System.out.println("Published: " + message);
                    
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }, 0, 2, TimeUnit.SECONDS);
        }
        
        static class SensorData {
            String timestamp;
            double data;
            String sensor;
            
            SensorData(String timestamp, double data, String sensor) {
                this.timestamp = timestamp;
                this.data = data;
                this.sensor = sensor;
            }
        }
    }
    
    static class MQTTSubscriber implements MqttCallback {
        private MqttClient client;
        private Gson gson = new Gson();
        
        public MQTTSubscriber(String brokerUrl) throws MqttException {
            client = new MqttClient(brokerUrl, "JavaSubscriber");
            client.setCallback(this);
            client.connect();
            client.subscribe("sensors/+");
        }
        
        @Override
        public void connectionLost(Throwable cause) {
            System.out.println("Connection lost: " + cause.getMessage());
        }
        
        @Override
        public void messageArrived(String topic, MqttMessage message) throws Exception {
            String payload = new String(message.getPayload());
            System.out.println("Received on " + topic + ": " + payload);
        }
        
        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
            // Not used for subscriber
        }
    }
    
    // ============ gRPC EXAMPLES ============
    
    // Note: In a real implementation, you would generate these from .proto files
    // using the protobuf compiler
    
    static class StreamingServiceImpl extends StreamingServiceGrpc.StreamingServiceImplBase {
        
        @Override
        public void serverStream(StreamRequest request, StreamObserver<StreamResponse> responseObserver) {
            System.out.println("Server streaming request: " + request.getMessage());
            
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            
            for (int i = 0; i < 10; i++) {
                final int count = i;
                executor.schedule(() -> {
                    StreamResponse response = StreamResponse.newBuilder()
                        .setMessage("Stream message " + count)
                        .setTimestamp(Instant.now().toString())
                        .setCount(count)
                        .build();
                    
                    responseObserver.onNext(response);
                    
                    if (count == 9) {
                        responseObserver.onCompleted();
                    }
                }, i, TimeUnit.SECONDS);
            }
        }
        
        @Override
        public StreamObserver<StreamRequest> bidirectionalStream(StreamObserver<StreamResponse> responseObserver) {
            return new StreamObserver<StreamRequest>() {
                @Override
                public void onNext(StreamRequest request) {
                    System.out.println("Received: " + request.getMessage());
                    
                    StreamResponse response = StreamResponse.newBuilder()
                        .setMessage("Echo: " + request.getMessage())
                        .setTimestamp(Instant.now().toString())
                        .build();
                    
                    responseObserver.onNext(response);
                }
                
                @Override
                public void onError(Throwable t) {
                    System.err.println("Error in bidirectional stream: " + t.getMessage());
                }
                
                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
    }
    
    static class GRPCServer {
        private Server server;
        
        public void start() throws Exception {
            server = ServerBuilder.forPort(50051)
                .addService(new StreamingServiceImpl())
                .build()
                .start();
            
            System.out.println("gRPC Server running on port 50051");
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.err.println("Shutting down gRPC server");
                try {
                    GRPCServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
            }));
        }
        
        public void stop() throws InterruptedException {
            if (server != null) {
                server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
            }
        }
        
        public void blockUntilShutdown() throws InterruptedException {
            if (server != null) {
                server.awaitTermination();
            }
        }
    }
    
    static class GRPCClient {
        private final ManagedChannel channel;
        private final StreamingServiceGrpc.StreamingServiceStub asyncStub;
        
        public GRPCClient(String host, int port) {
            channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
            asyncStub = StreamingServiceGrpc.newStub(channel);
        }
        
        public void serverStream() {
            StreamRequest request = StreamRequest.newBuilder()
                .setMessage("Start streaming")
                .build();
            
            asyncStub.serverStream(request, new StreamObserver<StreamResponse>() {
                @Override
                public void onNext(StreamResponse response) {
                    System.out.println("Server stream response: " + response.getMessage());
                }
                
                @Override
                public void onError(Throwable t) {
                    System.err.println("Server stream error: " + t.getMessage());
                }
                
                @Override
                public void onCompleted() {
                    System.out.println("Server stream completed");
                }
            });
        }
        
        public void bidirectionalStream() {
            StreamObserver<StreamRequest> requestObserver = asyncStub.bidirectionalStream(
                new StreamObserver<StreamResponse>() {
                    @Override
                    public void onNext(StreamResponse response) {
                        System.out.println("Bidirectional response: " + response.getMessage());
                    }
                    
                    @Override
                    public void onError(Throwable t) {
                        System.err.println("Bidirectional stream error: " + t.getMessage());
                    }
                    
                    @Override
                    public void onCompleted() {
                        System.out.println("Bidirectional stream completed");
                    }
                }
            );
            
            // Send messages
            for (int i = 0; i < 5; i++) {
                StreamRequest request = StreamRequest.newBuilder()
                    .setMessage("Client message " + i)
                    .setTimestamp(Instant.now().toString())
                    .build();
                
                requestObserver.onNext(request);
                
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            requestObserver.onCompleted();
        }
        
        public void shutdown() throws InterruptedException {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
    
    // Example usage
    public static void main(String[] args) throws Exception {
        System.out.println("Java MQTT & gRPC streaming examples ready");
        
        // MQTT Examples
        // MQTTPublisher publisher = new MQTTPublisher("tcp://localhost:1883");
        // publisher.publishSensorData();
        
        // MQTTSubscriber subscriber = new MQTTSubscriber("tcp://localhost:1883");
        
        // gRPC Examples
        // GRPCServer server = new GRPCServer();
        // server.start();
        
        // GRPCClient client = new GRPCClient("localhost", 50051);
        // client.serverStream();
        // client.bidirectionalStream();
    }
}