// C# - MQTT & gRPC Streaming Examples
// Dependencies: MQTTnet, Grpc.AspNetCore, Google.Protobuf

using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Server;
using Grpc.Core;
using Grpc.Net.Client;

namespace StreamingExamples
{
    // ============ MQTT EXAMPLES ============
    
    public class SensorData
    {
        public string Timestamp { get; set; }
        public double Data { get; set; }
        public string Sensor { get; set; }
    }
    
    public class MQTTPublisher
    {
        private IMqttClient _client;
        private Random _random = new Random();
        
        public async Task InitializeAsync(string brokerHost = "localhost", int brokerPort = 1883)
        {
            var factory = new MqttFactory();
            _client = factory.CreateMqttClient();
            
            var options = new MqttClientOptionsBuilder()
                .WithTcpServer(brokerHost, brokerPort)
                .WithClientId("CSharpPublisher")
                .Build();
                
            await _client.ConnectAsync(options);
        }
        
        public async Task PublishSensorDataAsync()
        {
            var timer = new Timer(async _ =>
            {
                var data = new SensorData
                {
                    Timestamp = DateTime.UtcNow.ToString("O"),
                    Data = _random.NextDouble() * 100,
                    Sensor = "temperature"
                };
                
                var json = JsonSerializer.Serialize(data);
                var message = new MqttApplicationMessageBuilder()
                    .WithTopic("sensors/temperature")
                    .WithPayload(json)
                    .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build();
                    
                await _client.PublishAsync(message);
                Console.WriteLine($"Published: {json}");
                
            }, null, TimeSpan.Zero, TimeSpan.FromSeconds(2));
        }
    }
    
    public class MQTTSubscriber
    {
        private IMqttClient _client;
        
        public async Task InitializeAsync(string brokerHost = "localhost", int brokerPort = 1883)
        {
            var factory = new MqttFactory();
            _client = factory.CreateMqttClient();
            
            _client.ApplicationMessageReceivedAsync += OnMessageReceivedAsync;
            
            var options = new MqttClientOptionsBuilder()
                .WithTcpServer(brokerHost, brokerPort)
                .WithClientId("CSharpSubscriber")
                .Build();
                
            await _client.ConnectAsync(options);
            
            await _client.SubscribeAsync(new MqttTopicFilterBuilder()
                .WithTopic("sensors/+")
                .Build());
                
            Console.WriteLine("MQTT Subscriber connected and listening...");
        }
        
        private Task OnMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs e)
        {
            var topic = e.ApplicationMessage.Topic;
            var payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);
            var data = JsonSerializer.Deserialize<SensorData>(payload);
            
            Console.WriteLine($"Received on {topic}: {JsonSerializer.Serialize(data)}");
            return Task.CompletedTask;
        }
    }
    
    // ============ gRPC EXAMPLES ============
    
    // Note: In a real implementation, you would generate these from .proto files
    // using the protobuf compiler
    
    public class StreamRequest
    {
        public string Message { get; set; }
        public string Timestamp { get; set; }
    }
    
    public class StreamResponse
    {
        public string Message { get; set; }
        public string Timestamp { get; set; }
        public int Count { get; set; }
    }
    
    // Mock gRPC service
    public class StreamingService : StreamingServiceBase
    {
        public override async Task ServerStream(StreamRequest request, 
            IServerStreamWriter<StreamResponse> responseStream, ServerCallContext context)
        {
            Console.WriteLine($"Server streaming request: {request.Message}");
            
            for (int i = 0; i < 10; i++)
            {
                var response = new StreamResponse
                {
                    Message = $"Stream message {i}",
                    Timestamp = DateTime.UtcNow.ToString("O"),
                    Count = i
                };
                
                await responseStream.WriteAsync(response);
                await Task.Delay(1000);
            }
        }
        
        public override async Task<Empty> BidirectionalStream(
            IAsyncStreamReader<StreamRequest> requestStream,
            IServerStreamWriter<StreamResponse> responseStream,
            ServerCallContext context)
        {
            await foreach (var request in requestStream.ReadAllAsync())
            {
                Console.WriteLine($"Received: {request.Message}");
                
                var response = new StreamResponse
                {
                    Message = $"Echo: {request.Message}",
                    Timestamp = DateTime.UtcNow.ToString("O")
                };
                
                await responseStream.WriteAsync(response);
            }
            
            return new Empty();
        }
    }
    
    public class GRPCServer
    {
        public async Task StartAsync()
        {
            var builder = WebApplication.CreateBuilder();
            builder.Services.AddGrpc();
            
            var app = builder.Build();
            app.MapGrpcService<StreamingService>();
            
            Console.WriteLine("gRPC Server running on port 50051");
            await app.RunAsync("http://localhost:50051");
        }
    }
    
    public class GRPCClient
    {
        private GrpcChannel _channel;
        private StreamingServiceClient _client;
        
        public void Initialize()
        {
            _channel = GrpcChannel.ForAddress("http://localhost:50051");
            _client = new StreamingServiceClient(_channel);
        }
        
        public async Task ServerStreamAsync()
        {
            var request = new StreamRequest 
            { 
                Message = "Start streaming",
                Timestamp = DateTime.UtcNow.ToString("O")
            };
            
            using var call = _client.ServerStream(request);
            
            await foreach (var response in call.ResponseStream.ReadAllAsync())
            {
                Console.WriteLine($"Server stream response: {response.Message}");
            }
        }
        
        public async Task BidirectionalStreamAsync()
        {
            using var call = _client.BidirectionalStream();
            
            // Start receiving responses
            var responseTask = Task.Run(async () =>
            {
                await foreach (var response in call.ResponseStream.ReadAllAsync())
                {
                    Console.WriteLine($"Bidirectional response: {response.Message}");
                }
            });
            
            // Send requests
            for (int i = 0; i < 5; i++)
            {
                var request = new StreamRequest
                {
                    Message = $"Client message {i}",
                    Timestamp = DateTime.UtcNow.ToString("O")
                };
                
                await call.RequestStream.WriteAsync(request);
                await Task.Delay(2000);
            }
            
            await call.RequestStream.CompleteAsync();
            await responseTask;
        }
        
        public async Task DisposeAsync()
        {
            await _channel.ShutdownAsync();
        }
    }
    
    // Example usage
    public class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("C# MQTT & gRPC streaming examples ready");
            
            // MQTT Examples
            // var publisher = new MQTTPublisher();
            // await publisher.InitializeAsync();
            // _ = publisher.PublishSensorDataAsync();
            
            // var subscriber = new MQTTSubscriber();
            // await subscriber.InitializeAsync();
            
            // gRPC Examples
            // var server = new GRPCServer();
            // _ = server.StartAsync();
            
            // await Task.Delay(1000); // Wait for server to start
            
            // var client = new GRPCClient();
            // client.Initialize();
            // await client.ServerStreamAsync();
            // await client.BidirectionalStreamAsync();
            // await client.DisposeAsync();
            
            Console.ReadLine();
        }
    }
}