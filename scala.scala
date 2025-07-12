// Scala - MQTT & gRPC Streaming Examples
// Dependencies: "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3", "io.grpc" % "grpc-netty"

import org.eclipse.paho.client.mqttv3._
import io.grpc._
import io.grpc.stub.StreamObserver
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, Materializer}
import spray.json._
import DefaultJsonProtocol._

import java.time.Instant
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Random

// ============ MQTT EXAMPLES ============

case class SensorData(timestamp: String, data: Double, sensor: String)

object SensorDataJsonProtocol extends DefaultJsonProtocol {
  implicit val sensorDataFormat = jsonFormat3(SensorData)
}

import SensorDataJsonProtocol._

class MQTTPublisher(brokerUrl: String) {
  private val client = new MqttClient(brokerUrl, "ScalaPublisher")
  private val random = new Random()
  
  def connect(): Unit = {
    val connOpts = new MqttConnectOptions()
    connOpts.setCleanSession(true)
    connOpts.setKeepAliveInterval(20)
    
    client.connect(connOpts)
    println("MQTT Publisher connected")
  }
  
  def publishSensorData()(implicit ec: ExecutionContext): Unit = {
    val scheduler = Executors.newScheduledThreadPool(1)
    
    scheduler.scheduleAtFixedRate(new Runnable {
      def run(): Unit = {
        val data = SensorData(
          timestamp = Instant.now().toString,
          data = random.nextDouble() * 100,
          sensor = "temperature"
        )
        
        val json = data.toJson.compactPrint
        val message = new MqttMessage(json.getBytes)
        message.setQos(1)
        
        try {
          client.publish("sensors/temperature", message)
          println(s"Published: $json")
        } catch {
          case e: MqttException => println(s"Error publishing: ${e.getMessage}")
        }
      }
    }, 0, 2, TimeUnit.SECONDS)
  }
  
  def disconnect(): Unit = {
    client.disconnect()
  }
}

class MQTTSubscriber(brokerUrl: String) extends MqttCallback {
  private val client = new MqttClient(brokerUrl, "ScalaSubscriber")
  
  def connect(): Unit = {
    client.setCallback(this)
    
    val connOpts = new MqttConnectOptions()
    connOpts.setCleanSession(true)
    connOpts.setKeepAliveInterval(20)
    
    client.connect(connOpts)
    client.subscribe("sensors/+")
    println("MQTT Subscriber connected and listening...")
  }
  
  override def connectionLost(cause: Throwable): Unit = {
    println(s"Connection lost: ${cause.getMessage}")
  }
  
  override def messageArrived(topic: String, message: MqttMessage): Unit = {
    try {
      val json = new String(message.getPayload)
      val data = json.parseJson.convertTo[SensorData]
      println(s"Received on $topic: timestamp=${data.timestamp}, data=${data.data}, sensor=${data.sensor}")
    } catch {
      case e: Exception => println(s"Error parsing message: ${e.getMessage}")
    }
  }
  
  override def deliveryComplete(token: IMqttDeliveryToken): Unit = {
    // Not used for subscriber
  }
  
  def disconnect(): Unit = {
    client.disconnect()
  }
}

// ============ gRPC EXAMPLES ============

// Note: In a real implementation, you would generate these from .proto files
// using ScalaPB: scalapb.ScalaPbPlugin

case class StreamRequest(message: String, timestamp: String)
case class StreamResponse(message: String, timestamp: String, count: Int = 0)

// Mock gRPC service implementation
class StreamingServiceImpl(implicit ec: ExecutionContext) {
  
  def serverStream(request: StreamRequest): Source[StreamResponse, _] = {
    println(s"Server streaming request: ${request.message}")
    
    Source(0 until 10)
      .throttle(1, 1.second)
      .map { i =>
        StreamResponse(
          message = s"Stream message $i",
          timestamp = Instant.now().toString,
          count = i
        )
      }
  }
  
  def bidirectionalStream(requests: Source[StreamRequest, _]): Source[StreamResponse, _] = {
    requests.map { request =>
      println(s"Received: ${request.message}")
      
      StreamResponse(
        message = s"Echo: ${request.message}",
        timestamp = Instant.now().toString
      )
    }
  }
}

class GRPCServer(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) {
  private var server: Option[Server] = None
  
  def start(): Unit = {
    // Note: This is a simplified example
    // In a real implementation, you would use the generated gRPC server code
    
    val serviceImpl = new StreamingServiceImpl()
    
    val serverBuilder = ServerBuilder.forPort(50051)
    // serverBuilder.addService(StreamingServiceGrpc.bindService(serviceImpl, ec))
    
    server = Some(serverBuilder.build().start())
    println("gRPC Server running on port 50051")
    
    sys.addShutdownHook {
      stop()
    }
  }
  
  def stop(): Unit = {
    server.foreach { s =>
      s.shutdown()
      if (!s.awaitTermination(30, TimeUnit.SECONDS)) {
        s.shutdownNow()
      }
    }
  }
  
  def blockUntilShutdown(): Unit = {
    server.foreach(_.awaitTermination())
  }
}

class GRPCClient(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext) {
  private val channel = ManagedChannelBuilder
    .forAddress("localhost", 50051)
    .usePlaintext()
    .build()
  
  // Note: In a real implementation, you would use the generated stub
  // private val stub = StreamingServiceGrpc.stub(channel)
  
  def serverStream(): Future[Unit] = {
    println("Starting server stream...")
    
    val request = StreamRequest("Start streaming", Instant.now().toString)
    
    // Mock server streaming call
    Source(0 until 10)
      .throttle(1, 1.second)
      .map(i => s"Server stream response: Stream message $i")
      .runForeach(println)
    
    // Real implementation would be:
    // stub.serverStream(request)
    //   .runForeach(response => println(s"Server stream response: ${response.message}"))
  }
  
  def bidirectionalStream(): Future[Unit] = {
    println("Starting bidirectional stream...")
    
    val requests = Source(0 until 5)
      .throttle(1, 2.seconds)
      .map { i =>
        StreamRequest(
          message = s"Client message $i",
          timestamp = Instant.now().toString
        )
      }
    
    // Mock bidirectional streaming
    requests
      .map { request =>
        println(s"Sending: ${request.message}")
        s"Bidirectional response: Echo: ${request.message}"
      }
      .runForeach(println)
    
    // Real implementation would be:
    // stub.bidirectionalStream(requests)
    //   .runForeach(response => println(s"Bidirectional response: ${response.message}"))
  }
  
  def shutdown(): Unit = {
    channel.shutdown()
    if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
      channel.shutdownNow()
    }
  }
}

// Example usage
object StreamingExamples extends App {
  implicit val system: ActorSystem = ActorSystem("StreamingSystem")
  implicit val materializer: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  
  println("Scala MQTT & gRPC streaming examples ready")
  
  // MQTT Examples
  // val publisher = new MQTTPublisher("tcp://localhost:1883")
  // publisher.connect()
  // publisher.publishSensorData()
  
  // val subscriber = new MQTTSubscriber("tcp://localhost:1883")
  // subscriber.connect()
  
  // gRPC Examples
  // val server = new GRPCServer()
  // server.start()
  
  // Thread.sleep(1000) // Wait for server to start
  
  // val client = new GRPCClient()
  // for {
  //   _ <- client.serverStream()
  //   _ <- client.bidirectionalStream()
  // } yield {
  //   client.shutdown()
  //   system.terminate()
  // }
  
  // Keep the application running
  scala.io.StdIn.readLine()
  system.terminate()
}