// JavaScript/Node.js - MQTT & gRPC Streaming Examples
// Dependencies: npm install mqtt @grpc/grpc-js @grpc/proto-loader

const mqtt = require('mqtt');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

// ============ MQTT EXAMPLES ============

// MQTT Publisher
function mqttPublisher() {
    const client = mqtt.connect('mqtt://localhost:1883');
    
    client.on('connect', () => {
        console.log('MQTT Publisher connected');
        
        // Publish messages every 2 seconds
        setInterval(() => {
            const message = {
                timestamp: new Date().toISOString(),
                data: Math.random() * 100,
                sensor: 'temperature'
            };
            
            client.publish('sensors/temperature', JSON.stringify(message));
            console.log('Published:', message);
        }, 2000);
    });
}

// MQTT Subscriber
function mqttSubscriber() {
    const client = mqtt.connect('mqtt://localhost:1883');
    
    client.on('connect', () => {
        console.log('MQTT Subscriber connected');
        client.subscribe('sensors/+', (err) => {
            if (!err) {
                console.log('Subscribed to sensors topics');
            }
        });
    });
    
    client.on('message', (topic, message) => {
        console.log(`Received on ${topic}:`, JSON.parse(message.toString()));
    });
}

// ============ gRPC EXAMPLES ============

// gRPC Server with Streaming
function grpcServer() {
    const packageDefinition = protoLoader.loadSync('streaming.proto');
    const streamingProto = grpc.loadPackageDefinition(packageDefinition).streaming;
    
    const server = new grpc.Server();
    
    // Server streaming - send multiple responses
    server.addService(streamingProto.StreamingService.service, {
        serverStream: (call) => {
            const request = call.request;
            console.log('Server streaming request:', request);
            
            let count = 0;
            const interval = setInterval(() => {
                call.write({
                    message: `Stream message ${count}`,
                    timestamp: new Date().toISOString(),
                    count: count
                });
                
                count++;
                if (count >= 10) {
                    clearInterval(interval);
                    call.end();
                }
            }, 1000);
        },
        
        // Bidirectional streaming
        bidirectionalStream: (call) => {
            call.on('data', (request) => {
                console.log('Received:', request);
                call.write({
                    message: `Echo: ${request.message}`,
                    timestamp: new Date().toISOString()
                });
            });
            
            call.on('end', () => {
                call.end();
            });
        }
    });
    
    server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {
        console.log('gRPC Server running on port 50051');
        server.start();
    });
}

// gRPC Client with Streaming
function grpcClient() {
    const packageDefinition = protoLoader.loadSync('streaming.proto');
    const streamingProto = grpc.loadPackageDefinition(packageDefinition).streaming;
    
    const client = new streamingProto.StreamingService('localhost:50051', 
        grpc.credentials.createInsecure());
    
    // Server streaming call
    const serverStream = client.serverStream({ message: 'Start streaming' });
    
    serverStream.on('data', (response) => {
        console.log('Server stream response:', response);
    });
    
    serverStream.on('end', () => {
        console.log('Server stream ended');
    });
    
    // Bidirectional streaming
    const biStream = client.bidirectionalStream();
    
    biStream.on('data', (response) => {
        console.log('Bidirectional response:', response);
    });
    
    // Send messages to server
    let messageCount = 0;
    const sendInterval = setInterval(() => {
        biStream.write({
            message: `Client message ${messageCount}`,
            timestamp: new Date().toISOString()
        });
        
        messageCount++;
        if (messageCount >= 5) {
            clearInterval(sendInterval);
            biStream.end();
        }
    }, 2000);
}

// Example usage
// mqttPublisher();
// mqttSubscriber();
// grpcServer();
// grpcClient();