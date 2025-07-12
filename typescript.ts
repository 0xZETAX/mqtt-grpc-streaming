// TypeScript - MQTT & gRPC Streaming Examples
// Dependencies: npm install mqtt @grpc/grpc-js @grpc/proto-loader

import * as mqtt from 'mqtt';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

// ============ MQTT EXAMPLES ============

interface SensorData {
  timestamp: string;
  data: number;
  sensor: string;
}

class MQTTPublisher {
  private client: mqtt.MqttClient;

  constructor(brokerUrl: string = 'mqtt://localhost:1883') {
    this.client = mqtt.connect(brokerUrl, {
      clientId: 'TypeScriptPublisher',
      keepalive: 20,
      clean: true,
    });

    this.client.on('connect', () => {
      console.log('MQTT Publisher connected');
    });

    this.client.on('error', (error) => {
      console.error('MQTT Publisher error:', error);
    });
  }

  public publishSensorData(): void {
    setInterval(() => {
      const data: SensorData = {
        timestamp: new Date().toISOString(),
        data: Math.random() * 100,
        sensor: 'temperature',
      };

      const message = JSON.stringify(data);
      this.client.publish('sensors/temperature', message, { qos: 1 }, (error) => {
        if (error) {
          console.error('Error publishing:', error);
        } else {
          console.log('Published:', message);
        }
      });
    }, 2000);
  }

  public disconnect(): void {
    this.client.end();
  }
}

class MQTTSubscriber {
  private client: mqtt.MqttClient;

  constructor(brokerUrl: string = 'mqtt://localhost:1883') {
    this.client = mqtt.connect(brokerUrl, {
      clientId: 'TypeScriptSubscriber',
      keepalive: 20,
      clean: true,
    });

    this.client.on('connect', () => {
      console.log('MQTT Subscriber connected');
      this.client.subscribe('sensors/+', { qos: 1 }, (error) => {
        if (!error) {
          console.log('MQTT Subscriber listening...');
        }
      });
    });

    this.client.on('message', (topic: string, message: Buffer) => {
      try {
        const data: SensorData = JSON.parse(message.toString());
        console.log(`Received on ${topic}: timestamp=${data.timestamp}, data=${data.data}, sensor=${data.sensor}`);
      } catch (error) {
        console.error('Error parsing message:', error);
      }
    });

    this.client.on('error', (error) => {
      console.error('MQTT Subscriber error:', error);
    });
  }

  public disconnect(): void {
    this.client.end();
  }
}

// ============ gRPC EXAMPLES ============

// Note: In a real implementation, you would generate these from .proto files
// using the protobuf compiler

interface StreamRequest {
  message: string;
  timestamp: string;
}

interface StreamResponse {
  message: string;
  timestamp: string;
  count?: number;
}

// Mock gRPC service interface
interface IStreamingService {
  serverStream(
    call: grpc.ServerWritableStream<StreamRequest, StreamResponse>
  ): void;
  
  bidirectionalStream(
    call: grpc.ServerDuplexStream<StreamRequest, StreamResponse>
  ): void;
}

// Mock gRPC service implementation
class StreamingServiceImpl implements IStreamingService {
  
  public serverStream(call: grpc.ServerWritableStream<StreamRequest, StreamResponse>): void {
    const request = call.request;
    console.log('Server streaming request:', request.message);

    let count = 0;
    const interval = setInterval(() => {
      const response: StreamResponse = {
        message: `Stream message ${count}`,
        timestamp: new Date().toISOString(),
        count: count,
      };

      call.write(response);
      count++;

      if (count >= 10) {
        clearInterval(interval);
        call.end();
      }
    }, 1000);
  }

  public bidirectionalStream(call: grpc.ServerDuplexStream<StreamRequest, StreamResponse>): void {
    call.on('data', (request: StreamRequest) => {
      console.log('Received:', request.message);

      const response: StreamResponse = {
        message: `Echo: ${request.message}`,
        timestamp: new Date().toISOString(),
      };

      call.write(response);
    });

    call.on('end', () => {
      call.end();
    });

    call.on('error', (error) => {
      console.error('Bidirectional stream error:', error);
    });
  }
}

class GRPCServer {
  private server: grpc.Server;

  constructor() {
    this.server = new grpc.Server();
  }

  public start(): void {
    // Note: This is a simplified example
    // In a real implementation, you would use the generated service definition
    
    const serviceImpl = new StreamingServiceImpl();
    
    // this.server.addService(StreamingServiceService, serviceImpl);

    this.server.bindAsync(
      '0.0.0.0:50051',
      grpc.ServerCredentials.createInsecure(),
      (error, port) => {
        if (error) {
          console.error('Failed to start server:', error);
          return;
        }

        console.log(`gRPC Server running on port ${port}`);
        this.server.start();
      }
    );
  }

  public stop(): void {
    this.server.forceShutdown();
  }
}

class GRPCClient {
  private client: any; // In real implementation, this would be the generated client type

  constructor() {
    // Note: This is a simplified example
    // In a real implementation, you would use the generated client
    
    const packageDefinition = protoLoader.loadSync('streaming.proto', {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });

    // const streamingProto = grpc.loadPackageDefinition(packageDefinition).streaming;
    // this.client = new streamingProto.StreamingService(
    //   'localhost:50051',
    //   grpc.credentials.createInsecure()
    // );

    console.log('gRPC Client connecting to localhost:50051');
  }

  public async serverStream(): Promise<void> {
    return new Promise((resolve, reject) => {
      console.log('Starting server stream...');

      const request: StreamRequest = {
        message: 'Start streaming',
        timestamp: new Date().toISOString(),
      };

      // Mock server streaming call
      let count = 0;
      const interval = setInterval(() => {
        console.log(`Server stream response: Stream message ${count}`);
        count++;

        if (count >= 10) {
          clearInterval(interval);
          resolve();
        }
      }, 1000);

      // Real implementation would be:
      // const call = this.client.serverStream(request);
      // 
      // call.on('data', (response: StreamResponse) => {
      //   console.log('Server stream response:', response.message);
      // });
      // 
      // call.on('end', () => {
      //   resolve();
      // });
      // 
      // call.on('error', (error: any) => {
      //   reject(error);
      // });
    });
  }

  public async bidirectionalStream(): Promise<void> {
    return new Promise((resolve, reject) => {
      console.log('Starting bidirectional stream...');

      // Mock bidirectional streaming
      let messageCount = 0;
      const sendInterval = setInterval(() => {
        const request: StreamRequest = {
          message: `Client message ${messageCount}`,
          timestamp: new Date().toISOString(),
        };

        console.log('Sending:', request.message);
        console.log('Bidirectional response: Echo:', request.message);

        messageCount++;
        if (messageCount >= 5) {
          clearInterval(sendInterval);
          resolve();
        }
      }, 2000);

      // Real implementation would be:
      // const call = this.client.bidirectionalStream();
      // 
      // call.on('data', (response: StreamResponse) => {
      //   console.log('Bidirectional response:', response.message);
      // });
      // 
      // call.on('end', () => {
      //   resolve();
      // });
      // 
      // call.on('error', (error: any) => {
      //   reject(error);
      // });
      // 
      // // Send messages
      // for (let i = 0; i < 5; i++) {
      //   setTimeout(() => {
      //     const request: StreamRequest = {
      //       message: `Client message ${i}`,
      //       timestamp: new Date().toISOString(),
      //     };
      //     call.write(request);
      //     
      //     if (i === 4) {
      //       call.end();
      //     }
      //   }, i * 2000);
      // }
    });
  }
}

// Example usage
async function main(): Promise<void> {
  console.log('TypeScript MQTT & gRPC streaming examples ready');

  // MQTT Examples
  // const publisher = new MQTTPublisher();
  // publisher.publishSensorData();

  // const subscriber = new MQTTSubscriber();

  // gRPC Examples
  // const server = new GRPCServer();
  // server.start();

  // await new Promise(resolve => setTimeout(resolve, 1000)); // Wait for server to start

  // const client = new GRPCClient();
  // await client.serverStream();
  // await client.bidirectionalStream();

  // Clean up
  // publisher.disconnect();
  // subscriber.disconnect();
  // server.stop();
}

if (require.main === module) {
  main().catch(console.error);
}

export { MQTTPublisher, MQTTSubscriber, GRPCServer, GRPCClient };