// Kotlin - MQTT & gRPC Streaming Examples
// Dependencies: org.eclipse.paho:org.eclipse.paho.client.mqttv3, io.grpc:grpc-kotlin-stub

import org.eclipse.paho.client.mqttv3.*
import io.grpc.*
import io.grpc.stub.StreamObserver
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.serialization.*
import kotlinx.serialization.json.*
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.random.Random

// ============ MQTT EXAMPLES ============

@Serializable
data class SensorData(
    val timestamp: String,
    val data: Double,
    val sensor: String
)

class MQTTPublisher(brokerUrl: String) {
    private val client: MqttAsyncClient = MqttAsyncClient(brokerUrl, "KotlinPublisher")
    private val json = Json { prettyPrint = true }
    
    init {
        val connOpts = MqttConnectOptions().apply {
            isCleanSession = true
            keepAliveInterval = 20
        }
        
        client.connect(connOpts).waitForCompletion()
        println("MQTT Publisher connected")
    }
    
    suspend fun publishSensorData() {
        while (true) {
            val data = SensorData(
                timestamp = Instant.now().toString(),
                data = Random.nextDouble(0.0, 100.0),
                sensor = "temperature"
            )
            
            val message = MqttMessage(json.encodeToString(data).toByteArray()).apply {
                qos = 1
            }
            
            try {
                client.publish("sensors/temperature", message).waitForCompletion()
                println("Published: ${json.encodeToString(data)}")
            } catch (e: MqttException) {
                println("Error publishing: ${e.message}")
            }
            
            delay(2000)
        }
    }
    
    fun disconnect() {
        client.disconnect()
    }
}

class MQTTSubscriber(brokerUrl: String) : MqttCallback {
    private val client: MqttAsyncClient = MqttAsyncClient(brokerUrl, "KotlinSubscriber")
    private val json = Json { ignoreUnknownKeys = true }
    
    init {
        client.setCallback(this)
        
        val connOpts = MqttConnectOptions().apply {
            isCleanSession = true
            keepAliveInterval = 20
        }
        
        client.connect(connOpts).waitForCompletion()
        client.subscribe("sensors/+", 1).waitForCompletion()
        println("MQTT Subscriber connected and listening...")
    }
    
    override fun connectionLost(cause: Throwable?) {
        println("Connection lost: ${cause?.message}")
    }
    
    override fun messageArrived(topic: String, message: MqttMessage) {
        try {
            val data = json.decodeFromString<SensorData>(message.toString())
            println("Received on $topic: timestamp=${data.timestamp}, data=${data.data}, sensor=${data.sensor}")
        } catch (e: Exception) {
            println("Error parsing message: ${e.message}")
        }
    }
    
    override fun deliveryComplete(token: IMqttDeliveryToken?) {
        // Not used for subscriber
    }
    
    fun disconnect() {
        client.disconnect()
    }
}

// ============ gRPC EXAMPLES ============

// Note: In a real implementation, you would generate these from .proto files
// using the protobuf compiler

data class StreamRequest(
    val message: String,
    val timestamp: String
)

data class StreamResponse(
    val message: String,
    val timestamp: String,
    val count: Int = 0
)

// Mock gRPC service implementation
class StreamingServiceImpl {
    
    fun serverStream(request: StreamRequest): Flow<StreamResponse> = flow {
        println("Server streaming request: ${request.message}")
        
        repeat(10) { i ->
            val response = StreamResponse(
                message = "Stream message $i",
                timestamp = Instant.now().toString(),
                count = i
            )
            
            emit(response)
            delay(1000)
        }
    }
    
    fun bidirectionalStream(requests: Flow<StreamRequest>): Flow<StreamResponse> = flow {
        requests.collect { request ->
            println("Received: ${request.message}")
            
            val response = StreamResponse(
                message = "Echo: ${request.message}",
                timestamp = Instant.now().toString()
            )
            
            emit(response)
        }
    }
}

class GRPCServer {
    private lateinit var server: Server
    
    fun start() {
        server = ServerBuilder.forPort(50051)
            .addService(StreamingServiceImpl())
            .build()
            .start()
        
        println("gRPC Server running on port 50051")
        
        Runtime.getRuntime().addShutdownHook(Thread {
            println("Shutting down gRPC server")
            stop()
        })
    }
    
    private fun stop() {
        server.shutdown().awaitTermination(30, TimeUnit.SECONDS)
    }
    
    fun blockUntilShutdown() {
        server.awaitTermination()
    }
}

class GRPCClient {
    private val channel: ManagedChannel = ManagedChannelBuilder
        .forAddress("localhost", 50051)
        .usePlaintext()
        .build()
    
    // Note: In a real implementation, you would use the generated stub
    // private val stub = StreamingServiceGrpcKt.StreamingServiceCoroutineStub(channel)
    
    suspend fun serverStream() {
        println("Starting server stream...")
        
        val request = StreamRequest("Start streaming", Instant.now().toString())
        
        // Mock server streaming call
        repeat(10) { i ->
            println("Server stream response: Stream message $i")
            delay(1000)
        }
        
        // Real implementation would be:
        // stub.serverStream(request).collect { response ->
        //     println("Server stream response: ${response.message}")
        // }
    }
    
    suspend fun bidirectionalStream() {
        println("Starting bidirectional stream...")
        
        // Mock bidirectional streaming
        val requests = flow {
            repeat(5) { i ->
                val request = StreamRequest(
                    message = "Client message $i",
                    timestamp = Instant.now().toString()
                )
                emit(request)
                delay(2000)
            }
        }
        
        requests.collect { request ->
            println("Sending: ${request.message}")
            println("Bidirectional response: Echo: ${request.message}")
        }
        
        // Real implementation would be:
        // stub.bidirectionalStream(requests).collect { response ->
        //     println("Bidirectional response: ${response.message}")
        // }
    }
    
    fun shutdown() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}

// Example usage
suspend fun main() {
    println("Kotlin MQTT & gRPC streaming examples ready")
    
    // MQTT Examples
    // val publisher = MQTTPublisher("tcp://localhost:1883")
    // val publisherJob = GlobalScope.launch { publisher.publishSensorData() }
    
    // val subscriber = MQTTSubscriber("tcp://localhost:1883")
    
    // gRPC Examples
    // val server = GRPCServer()
    // val serverJob = GlobalScope.launch { server.start() }
    
    // delay(1000) // Wait for server to start
    
    // val client = GRPCClient()
    // client.serverStream()
    // client.bidirectionalStream()
    // client.shutdown()
    
    // Clean up
    // publisher.disconnect()
    // subscriber.disconnect()
    // publisherJob.cancel()
    // serverJob.cancel()
}