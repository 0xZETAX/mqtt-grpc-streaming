// Swift - MQTT & gRPC Streaming Examples
// Dependencies: CocoaMQTT, grpc-swift

import Foundation
import CocoaMQTT
import GRPC
import NIO
import NIOHPACK
import Logging

// ============ MQTT EXAMPLES ============

struct SensorData: Codable {
    let timestamp: String
    let data: Double
    let sensor: String
}

class MQTTPublisher {
    private let mqtt: CocoaMQTT
    private let encoder = JSONEncoder()
    
    init(brokerHost: String = "localhost", brokerPort: UInt16 = 1883) {
        let clientID = "SwiftPublisher-" + String(ProcessInfo().processIdentifier)
        mqtt = CocoaMQTT(clientID: clientID, host: brokerHost, port: brokerPort)
        
        mqtt.willMessage = CocoaMQTTWill(topic: "/will", message: "dieout")
        mqtt.keepAlive = 60
        mqtt.delegate = self
        
        _ = mqtt.connect()
        print("MQTT Publisher connecting...")
    }
    
    func publishSensorData() {
        Timer.scheduledTimer(withTimeInterval: 2.0, repeats: true) { _ in
            let data = SensorData(
                timestamp: ISO8601DateFormatter().string(from: Date()),
                data: Double.random(in: 0...100),
                sensor: "temperature"
            )
            
            do {
                let jsonData = try self.encoder.encode(data)
                let jsonString = String(data: jsonData, encoding: .utf8) ?? ""
                
                self.mqtt.publish("sensors/temperature", withString: jsonString, qos: .qos1)
                print("Published: \(jsonString)")
            } catch {
                print("Error encoding sensor data: \(error)")
            }
        }
    }
    
    func disconnect() {
        mqtt.disconnect()
    }
}

extension MQTTPublisher: CocoaMQTTDelegate {
    func mqtt(_ mqtt: CocoaMQTT, didConnectAck ack: CocoaMQTTConnAck) {
        print("MQTT Publisher connected with ack: \(ack)")
    }
    
    func mqtt(_ mqtt: CocoaMQTT, didPublishMessage message: CocoaMQTTMessage, id: UInt16) {
        // Message published successfully
    }
    
    func mqtt(_ mqtt: CocoaMQTT, didPublishAck id: UInt16) {
        // Publish acknowledged
    }
    
    func mqtt(_ mqtt: CocoaMQTT, didReceiveMessage message: CocoaMQTTMessage, id: UInt16) {
        // Not used for publisher
    }
    
    func mqtt(_ mqtt: CocoaMQTT, didSubscribeTopics success: NSDictionary, failed: [String]) {
        // Not used for publisher
    }
    
    func mqtt(_ mqtt: CocoaMQTT, didUnsubscribeTopics topics: [String]) {
        // Not used for publisher
    }
    
    func mqttDidPing(_ mqtt: CocoaMQTT) {
        // Ping sent
    }
    
    func mqttDidReceivePong(_ mqtt: CocoaMQTT) {
        // Pong received
    }
    
    func mqttDidDisconnect(_ mqtt: CocoaMQTT, withError err: Error?) {
        print("MQTT Publisher disconnected: \(err?.localizedDescription ?? "No error")")
    }
}

class MQTTSubscriber {
    private let mqtt: CocoaMQTT
    private let decoder = JSONDecoder()
    
    init(brokerHost: String = "localhost", brokerPort: UInt16 = 1883) {
        let clientID = "SwiftSubscriber-" + String(ProcessInfo().processIdentifier)
        mqtt = CocoaMQTT(clientID: clientID, host: brokerHost, port: brokerPort)
        
        mqtt.willMessage = CocoaMQTTWill(topic: "/will", message: "dieout")
        mqtt.keepAlive = 60
        mqtt.delegate = self
        
        _ = mqtt.connect()
        print("MQTT Subscriber connecting...")
    }
    
    func disconnect() {
        mqtt.disconnect()
    }
}

extension MQTTSubscriber: CocoaMQTTDelegate {
    func mqtt(_ mqtt: CocoaMQTT, didConnectAck ack: CocoaMQTTConnAck) {
        print("MQTT Subscriber connected with ack: \(ack)")
        mqtt.subscribe("sensors/+", qos: .qos1)
        print("MQTT Subscriber listening...")
    }
    
    func mqtt(_ mqtt: CocoaMQTT, didPublishMessage message: CocoaMQTTMessage, id: UInt16) {
        // Not used for subscriber
    }
    
    func mqtt(_ mqtt: CocoaMQTT, didPublishAck id: UInt16) {
        // Not used for subscriber
    }
    
    func mqtt(_ mqtt: CocoaMQTT, didReceiveMessage message: CocoaMQTTMessage, id: UInt16) {
        guard let messageString = message.string else { return }
        
        do {
            let data = try decoder.decode(SensorData.self, from: messageString.data(using: .utf8)!)
            print("Received on \(message.topic): timestamp=\(data.timestamp), data=\(data.data), sensor=\(data.sensor)")
        } catch {
            print("Error decoding message: \(error)")
        }
    }
    
    func mqtt(_ mqtt: CocoaMQTT, didSubscribeTopics success: NSDictionary, failed: [String]) {
        print("Subscribed to topics: \(success)")
    }
    
    func mqtt(_ mqtt: CocoaMQTT, didUnsubscribeTopics topics: [String]) {
        print("Unsubscribed from topics: \(topics)")
    }
    
    func mqttDidPing(_ mqtt: CocoaMQTT) {
        // Ping sent
    }
    
    func mqttDidReceivePong(_ mqtt: CocoaMQTT) {
        // Pong received
    }
    
    func mqttDidDisconnect(_ mqtt: CocoaMQTT, withError err: Error?) {
        print("MQTT Subscriber disconnected: \(err?.localizedDescription ?? "No error")")
    }
}

// ============ gRPC EXAMPLES ============

// Note: In a real implementation, you would generate these from .proto files
// using the protobuf compiler

struct StreamRequest {
    let message: String
    let timestamp: String
}

struct StreamResponse {
    let message: String
    let timestamp: String
    let count: Int
}

// Mock gRPC service implementation
class StreamingServiceProvider {
    
    func serverStream(request: StreamRequest) -> EventLoopFuture<[StreamResponse]> {
        print("Server streaming request: \(request.message)")
        
        let eventLoop = MultiThreadedEventLoopGroup(numberOfThreads: 1).next()
        let promise = eventLoop.makePromise(of: [StreamResponse].self)
        
        var responses: [StreamResponse] = []
        
        for i in 0..<10 {
            let response = StreamResponse(
                message: "Stream message \(i)",
                timestamp: ISO8601DateFormatter().string(from: Date()),
                count: i
            )
            responses.append(response)
        }
        
        promise.succeed(responses)
        return promise.futureResult
    }
    
    func bidirectionalStream(requests: [StreamRequest]) -> EventLoopFuture<[StreamResponse]> {
        let eventLoop = MultiThreadedEventLoopGroup(numberOfThreads: 1).next()
        let promise = eventLoop.makePromise(of: [StreamResponse].self)
        
        var responses: [StreamResponse] = []
        
        for request in requests {
            print("Received: \(request.message)")
            
            let response = StreamResponse(
                message: "Echo: \(request.message)",
                timestamp: ISO8601DateFormatter().string(from: Date()),
                count: 0
            )
            responses.append(response)
        }
        
        promise.succeed(responses)
        return promise.futureResult
    }
}

class GRPCServer {
    private var server: Server?
    private let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    
    func start() throws {
        // Note: This is a simplified example
        // In a real implementation, you would use the generated gRPC server code
        
        print("gRPC Server running on port 50051")
        
        // Mock server implementation
        let serviceProvider = StreamingServiceProvider()
        
        // Keep the server running
        try group.next().makeSucceededFuture(()).wait()
    }
    
    func stop() {
        try? group.syncShutdownGracefully()
    }
}

class GRPCClient {
    private let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    private var channel: ClientConnection?
    
    init() {
        // Note: This is a simplified example
        // In a real implementation, you would use the generated gRPC client code
        
        print("gRPC Client connecting to localhost:50051")
        
        channel = ClientConnection.insecure(group: group)
            .connect(host: "localhost", port: 50051)
    }
    
    func serverStream() {
        print("Starting server stream...")
        
        let request = StreamRequest(
            message: "Start streaming",
            timestamp: ISO8601DateFormatter().string(from: Date())
        )
        
        // Mock server streaming call
        for i in 0..<10 {
            print("Server stream response: Stream message \(i)")
            Thread.sleep(forTimeInterval: 1.0)
        }
    }
    
    func bidirectionalStream() {
        print("Starting bidirectional stream...")
        
        // Mock bidirectional streaming
        for i in 0..<5 {
            let request = StreamRequest(
                message: "Client message \(i)",
                timestamp: ISO8601DateFormatter().string(from: Date())
            )
            
            print("Sending: \(request.message)")
            print("Bidirectional response: Echo: \(request.message)")
            Thread.sleep(forTimeInterval: 2.0)
        }
    }
    
    func shutdown() {
        try? channel?.close().wait()
        try? group.syncShutdownGracefully()
    }
}

// Example usage
func main() {
    print("Swift MQTT & gRPC streaming examples ready")
    
    // MQTT Examples
    // let publisher = MQTTPublisher()
    // publisher.publishSensorData()
    
    // let subscriber = MQTTSubscriber()
    
    // gRPC Examples
    // let server = GRPCServer()
    // DispatchQueue.global().async {
    //     try? server.start()
    // }
    
    // Thread.sleep(forTimeInterval: 1.0) // Wait for server to start
    
    // let client = GRPCClient()
    // client.serverStream()
    // client.bidirectionalStream()
    // client.shutdown()
    
    // Keep the main thread alive
    RunLoop.main.run()
}

if CommandLine.argc > 0 {
    main()
}