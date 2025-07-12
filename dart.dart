// Dart - MQTT & gRPC Streaming Examples
// Dependencies: mqtt_client, grpc

import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'package:mqtt_client/mqtt_client.dart';
import 'package:mqtt_client/mqtt_server_client.dart';
import 'package:grpc/grpc.dart';

// ============ MQTT EXAMPLES ============

class SensorData {
  final String timestamp;
  final double data;
  final String sensor;
  
  SensorData({
    required this.timestamp,
    required this.data,
    required this.sensor,
  });
  
  Map<String, dynamic> toJson() => {
    'timestamp': timestamp,
    'data': data,
    'sensor': sensor,
  };
  
  factory SensorData.fromJson(Map<String, dynamic> json) => SensorData(
    timestamp: json['timestamp'],
    data: json['data'].toDouble(),
    sensor: json['sensor'],
  );
}

class MQTTPublisher {
  late MqttServerClient client;
  final Random _random = Random();
  
  Future<void> connect({String brokerHost = 'localhost', int brokerPort = 1883}) async {
    client = MqttServerClient(brokerHost, 'DartPublisher');
    client.port = brokerPort;
    client.keepAlivePeriod = 20;
    client.autoReconnect = true;
    
    try {
      await client.connect();
      print('MQTT Publisher connected');
    } catch (e) {
      print('Error connecting: $e');
      client.disconnect();
    }
  }
  
  void publishSensorData() {
    Timer.periodic(Duration(seconds: 2), (timer) async {
      if (client.connectionStatus?.state == MqttConnectionState.connected) {
        final data = SensorData(
          timestamp: DateTime.now().toIso8601String(),
          data: _random.nextDouble() * 100,
          sensor: 'temperature',
        );
        
        final json = jsonEncode(data.toJson());
        final builder = MqttClientPayloadBuilder();
        builder.addString(json);
        
        client.publishMessage(
          'sensors/temperature',
          MqttQos.atLeastOnce,
          builder.payload!,
        );
        
        print('Published: $json');
      }
    });
  }
  
  void disconnect() {
    client.disconnect();
  }
}

class MQTTSubscriber {
  late MqttServerClient client;
  
  Future<void> connect({String brokerHost = 'localhost', int brokerPort = 1883}) async {
    client = MqttServerClient(brokerHost, 'DartSubscriber');
    client.port = brokerPort;
    client.keepAlivePeriod = 20;
    client.autoReconnect = true;
    
    client.updates!.listen((List<MqttReceivedMessage<MqttMessage>> c) {
      final message = c[0].payload as MqttPublishMessage;
      final payload = MqttPublishPayload.bytesToStringAsString(message.payload.message);
      
      try {
        final json = jsonDecode(payload) as Map<String, dynamic>;
        final data = SensorData.fromJson(json);
        print('Received on ${c[0].topic}: timestamp=${data.timestamp}, data=${data.data}, sensor=${data.sensor}');
      } catch (e) {
        print('Error parsing message: $e');
      }
    });
    
    try {
      await client.connect();
      client.subscribe('sensors/+', MqttQos.atLeastOnce);
      print('MQTT Subscriber connected and listening...');
    } catch (e) {
      print('Error connecting: $e');
      client.disconnect();
    }
  }
  
  void disconnect() {
    client.disconnect();
  }
}

// ============ gRPC EXAMPLES ============

// Note: In a real implementation, you would generate these from .proto files
// using: protoc --dart_out=grpc:lib/src/generated -Iprotos protos/streaming.proto

class StreamRequest {
  final String message;
  final String timestamp;
  
  StreamRequest({required this.message, required this.timestamp});
}

class StreamResponse {
  final String message;
  final String timestamp;
  final int count;
  
  StreamResponse({
    required this.message,
    required this.timestamp,
    this.count = 0,
  });
}

// Mock gRPC service implementation
class StreamingServiceImpl {
  
  Stream<StreamResponse> serverStream(StreamRequest request) async* {
    print('Server streaming request: ${request.message}');
    
    for (int i = 0; i < 10; i++) {
      yield StreamResponse(
        message: 'Stream message $i',
        timestamp: DateTime.now().toIso8601String(),
        count: i,
      );
      
      await Future.delayed(Duration(seconds: 1));
    }
  }
  
  Stream<StreamResponse> bidirectionalStream(Stream<StreamRequest> requests) async* {
    await for (final request in requests) {
      print('Received: ${request.message}');
      
      yield StreamResponse(
        message: 'Echo: ${request.message}',
        timestamp: DateTime.now().toIso8601String(),
      );
    }
  }
}

class GRPCServer {
  late Server server;
  
  Future<void> start() async {
    // Note: This is a simplified example
    // In a real implementation, you would use the generated gRPC server code
    
    final serviceImpl = StreamingServiceImpl();
    
    server = Server([
      // StreamingServiceBase(serviceImpl)
    ]);
    
    await server.serve(port: 50051);
    print('gRPC Server running on port 50051');
  }
  
  Future<void> stop() async {
    await server.shutdown();
  }
}

class GRPCClient {
  late ClientChannel channel;
  // late StreamingServiceClient stub;
  
  void connect() {
    channel = ClientChannel(
      'localhost',
      port: 50051,
      options: ChannelOptions(
        credentials: ChannelCredentials.insecure(),
      ),
    );
    
    // stub = StreamingServiceClient(channel);
    print('gRPC Client connecting to localhost:50051');
  }
  
  Future<void> serverStream() async {
    print('Starting server stream...');
    
    final request = StreamRequest(
      message: 'Start streaming',
      timestamp: DateTime.now().toIso8601String(),
    );
    
    // Mock server streaming call
    for (int i = 0; i < 10; i++) {
      print('Server stream response: Stream message $i');
      await Future.delayed(Duration(seconds: 1));
    }
    
    // Real implementation would be:
    // final responses = stub.serverStream(request);
    // await for (final response in responses) {
    //   print('Server stream response: ${response.message}');
    // }
  }
  
  Future<void> bidirectionalStream() async {
    print('Starting bidirectional stream...');
    
    // Mock bidirectional streaming
    final controller = StreamController<StreamRequest>();
    
    // Send requests
    Timer.periodic(Duration(seconds: 2), (timer) {
      if (timer.tick <= 5) {
        final request = StreamRequest(
          message: 'Client message ${timer.tick - 1}',
          timestamp: DateTime.now().toIso8601String(),
        );
        controller.add(request);
        print('Sending: ${request.message}');
        print('Bidirectional response: Echo: ${request.message}');
      } else {
        timer.cancel();
        controller.close();
      }
    });
    
    // Real implementation would be:
    // final responses = stub.bidirectionalStream(controller.stream);
    // await for (final response in responses) {
    //   print('Bidirectional response: ${response.message}');
    // }
    
    await controller.done;
  }
  
  Future<void> shutdown() async {
    await channel.shutdown();
  }
}

// Example usage
Future<void> main() async {
  print('Dart MQTT & gRPC streaming examples ready');
  
  // MQTT Examples
  // final publisher = MQTTPublisher();
  // await publisher.connect();
  // publisher.publishSensorData();
  
  // final subscriber = MQTTSubscriber();
  // await subscriber.connect();
  
  // gRPC Examples
  // final server = GRPCServer();
  // unawaited(server.start());
  
  // await Future.delayed(Duration(seconds: 1)); // Wait for server to start
  
  // final client = GRPCClient();
  // client.connect();
  // await client.serverStream();
  // await client.bidirectionalStream();
  // await client.shutdown();
  
  // Clean up
  // publisher.disconnect();
  // subscriber.disconnect();
  // await server.stop();
}