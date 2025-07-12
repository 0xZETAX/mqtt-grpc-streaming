// Rust - MQTT & gRPC Streaming Examples
// Dependencies: tokio, paho-mqtt, tonic, prost, serde_json

use std::time::Duration;
use tokio::time::interval;
use serde::{Deserialize, Serialize};
use paho_mqtt as mqtt;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc;

// ============ MQTT EXAMPLES ============

#[derive(Serialize, Deserialize, Debug)]
struct SensorData {
    timestamp: String,
    data: f64,
    sensor: String,
}

struct MQTTPublisher {
    client: mqtt::AsyncClient,
}

impl MQTTPublisher {
    async fn new(broker_url: &str) -> Result<Self, mqtt::Error> {
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(broker_url)
            .client_id("RustPublisher")
            .finalize();
            
        let client = mqtt::AsyncClient::new(create_opts)?;
        
        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(true)
            .finalize();
            
        client.connect(conn_opts).await?;
        
        Ok(MQTTPublisher { client })
    }
    
    async fn publish_sensor_data(&self) {
        let mut interval = interval(Duration::from_secs(2));
        
        loop {
            interval.tick().await;
            
            let data = SensorData {
                timestamp: chrono::Utc::now().to_rfc3339(),
                data: rand::random::<f64>() * 100.0,
                sensor: "temperature".to_string(),
            };
            
            let json = serde_json::to_string(&data).unwrap();
            let msg = mqtt::MessageBuilder::new()
                .topic("sensors/temperature")
                .payload(json.clone())
                .qos(1)
                .finalize();
                
            if let Err(e) = self.client.publish(msg).await {
                eprintln!("Error publishing message: {}", e);
            } else {
                println!("Published: {}", json);
            }
        }
    }
}

struct MQTTSubscriber {
    client: mqtt::AsyncClient,
}

impl MQTTSubscriber {
    async fn new(broker_url: &str) -> Result<Self, mqtt::Error> {
        let create_opts = mqtt::CreateOptionsBuilder::new()
            .server_uri(broker_url)
            .client_id("RustSubscriber")
            .finalize();
            
        let client = mqtt::AsyncClient::new(create_opts)?;
        
        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(true)
            .finalize();
            
        client.connect(conn_opts).await?;
        client.subscribe("sensors/+", 1).await?;
        
        Ok(MQTTSubscriber { client })
    }
    
    async fn start_listening(&self) {
        let mut stream = self.client.get_stream(25);
        
        println!("MQTT Subscriber connected and listening...");
        
        while let Some(msg_opt) = stream.next().await {
            if let Some(msg) = msg_opt {
                let topic = msg.topic();
                let payload = msg.payload_str();
                
                if let Ok(data) = serde_json::from_str::<SensorData>(&payload) {
                    println!("Received on {}: {:?}", topic, data);
                }
            }
        }
    }
}

// ============ gRPC EXAMPLES ============

// Note: In a real implementation, you would generate these from .proto files
// using tonic-build in build.rs

#[derive(Debug)]
pub struct StreamRequest {
    pub message: String,
    pub timestamp: String,
}

#[derive(Debug)]
pub struct StreamResponse {
    pub message: String,
    pub timestamp: String,
    pub count: i32,
}

// Mock gRPC service trait
#[tonic::async_trait]
pub trait StreamingService: Send + Sync + 'static {
    type ServerStreamStream: futures::Stream<Item = Result<StreamResponse, Status>>
        + Send
        + 'static;
        
    async fn server_stream(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::ServerStreamStream>, Status>;
    
    async fn bidirectional_stream(
        &self,
        request: Request<Streaming<StreamRequest>>,
    ) -> Result<Response<Streaming<StreamResponse>>, Status>;
}

pub struct StreamingServiceImpl;

#[tonic::async_trait]
impl StreamingService for StreamingServiceImpl {
    type ServerStreamStream = ReceiverStream<Result<StreamResponse, Status>>;
    
    async fn server_stream(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::ServerStreamStream>, Status> {
        println!("Server streaming request: {:?}", request.get_ref());
        
        let (tx, rx) = mpsc::channel(4);
        
        tokio::spawn(async move {
            for i in 0..10 {
                let response = StreamResponse {
                    message: format!("Stream message {}", i),
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    count: i,
                };
                
                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
                
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        
        Ok(Response::new(ReceiverStream::new(rx)))
    }
    
    async fn bidirectional_stream(
        &self,
        request: Request<Streaming<StreamRequest>>,
    ) -> Result<Response<Streaming<StreamResponse>>, Status> {
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        
        tokio::spawn(async move {
            while let Some(request) = in_stream.next().await {
                match request {
                    Ok(req) => {
                        println!("Received: {:?}", req);
                        
                        let response = StreamResponse {
                            message: format!("Echo: {}", req.message),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            count: 0,
                        };
                        
                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        eprintln!("Error receiving request: {}", err);
                        break;
                    }
                }
            }
        });
        
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn run_grpc_server() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let streaming_service = StreamingServiceImpl;
    
    println!("gRPC Server running on port 50051");
    
    Server::builder()
        .add_service(StreamingServiceServer::new(streaming_service))
        .serve(addr)
        .await?;
        
    Ok(())
}

async fn run_grpc_client() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = StreamingServiceClient::connect("http://[::1]:50051").await?;
    
    // Server streaming call
    println!("Starting server stream...");
    let request = tonic::Request::new(StreamRequest {
        message: "Start streaming".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    });
    
    let mut stream = client.server_stream(request).await?.into_inner();
    
    while let Some(response) = stream.next().await {
        match response {
            Ok(resp) => println!("Server stream response: {:?}", resp),
            Err(e) => eprintln!("Error: {}", e),
        }
    }
    
    // Bidirectional streaming
    println!("Starting bidirectional stream...");
    let (tx, rx) = mpsc::channel(4);
    let request_stream = ReceiverStream::new(rx);
    
    let mut response_stream = client
        .bidirectional_stream(Request::new(request_stream))
        .await?
        .into_inner();
    
    // Send requests
    tokio::spawn(async move {
        for i in 0..5 {
            let request = StreamRequest {
                message: format!("Client message {}", i),
                timestamp: chrono::Utc::now().to_rfc3339(),
            };
            
            if tx.send(request).await.is_err() {
                break;
            }
            
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    });
    
    // Receive responses
    while let Some(response) = response_stream.next().await {
        match response {
            Ok(resp) => println!("Bidirectional response: {:?}", resp),
            Err(e) => eprintln!("Error: {}", e),
        }
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Rust MQTT & gRPC streaming examples ready");
    
    // MQTT Examples
    // let publisher = MQTTPublisher::new("tcp://localhost:1883").await?;
    // tokio::spawn(async move {
    //     publisher.publish_sensor_data().await;
    // });
    
    // let subscriber = MQTTSubscriber::new("tcp://localhost:1883").await?;
    // tokio::spawn(async move {
    //     subscriber.start_listening().await;
    // });
    
    // gRPC Examples
    // tokio::spawn(async {
    //     if let Err(e) = run_grpc_server().await {
    //         eprintln!("Server error: {}", e);
    //     }
    // });
    
    // tokio::time::sleep(Duration::from_secs(1)).await;
    
    // if let Err(e) = run_grpc_client().await {
    //     eprintln!("Client error: {}", e);
    // }
    
    Ok(())
}