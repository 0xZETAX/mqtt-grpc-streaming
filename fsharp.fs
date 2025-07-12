// F# - MQTT & gRPC Streaming Examples
// Dependencies: MQTTnet, Grpc.AspNetCore, FSharp.SystemTextJson

open System
open System.Text
open System.Text.Json
open System.Threading
open System.Threading.Tasks
open MQTTnet
open MQTTnet.Client
open Grpc.Core
open Grpc.Net.Client

// ============ MQTT EXAMPLES ============

type SensorData = {
    Timestamp: string
    Data: float
    Sensor: string
}

module SensorData =
    let create data sensor =
        {
            Timestamp = DateTime.UtcNow.ToString("O")
            Data = data
            Sensor = sensor
        }
    
    let toJson (data: SensorData) =
        JsonSerializer.Serialize(data)
    
    let fromJson (json: string) =
        try
            JsonSerializer.Deserialize<SensorData>(json) |> Some
        with
        | ex -> 
            printfn "Error parsing JSON: %s" ex.Message
            None

type MQTTPublisher(brokerHost: string, brokerPort: int) =
    let mutable client: IMqttClient option = None
    let random = Random()
    
    member this.ConnectAsync() = async {
        let factory = MqttFactory()
        let mqttClient = factory.CreateMqttClient()
        
        let options = 
            MqttClientOptionsBuilder()
                .WithTcpServer(brokerHost, brokerPort)
                .WithClientId("FSharpPublisher")
                .Build()
        
        do! mqttClient.ConnectAsync(options) |> Async.AwaitTask
        client <- Some mqttClient
        printfn "MQTT Publisher connected"
    }
    
    member this.PublishSensorDataAsync() = async {
        match client with
        | Some mqttClient ->
            let rec publishLoop() = async {
                let data = SensorData.create (random.NextDouble() * 100.0) "temperature"
                let json = SensorData.toJson data
                
                let message = 
                    MqttApplicationMessageBuilder()
                        .WithTopic("sensors/temperature")
                        .WithPayload(json)
                        .WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build()
                
                try
                    do! mqttClient.PublishAsync(message) |> Async.AwaitTask
                    printfn "Published: %s" json
                with
                | ex -> printfn "Error publishing: %s" ex.Message
                
                do! Async.Sleep(2000)
                return! publishLoop()
            }
            return! publishLoop()
        | None -> 
            printfn "Client not connected"
    }
    
    member this.Disconnect() =
        match client with
        | Some mqttClient -> mqttClient.DisconnectAsync() |> ignore
        | None -> ()

type MQTTSubscriber(brokerHost: string, brokerPort: int) =
    let mutable client: IMqttClient option = None
    
    member this.ConnectAsync() = async {
        let factory = MqttFactory()
        let mqttClient = factory.CreateMqttClient()
        
        mqttClient.ApplicationMessageReceivedAsync.AddHandler(fun args ->
            let topic = args.ApplicationMessage.Topic
            let payload = Encoding.UTF8.GetString(args.ApplicationMessage.Payload)
            
            match SensorData.fromJson payload with
            | Some data ->
                printfn "Received on %s: timestamp=%s, data=%f, sensor=%s" 
                    topic data.Timestamp data.Data data.Sensor
            | None -> ()
            
            Task.CompletedTask
        )
        
        let options = 
            MqttClientOptionsBuilder()
                .WithTcpServer(brokerHost, brokerPort)
                .WithClientId("FSharpSubscriber")
                .Build()
        
        do! mqttClient.ConnectAsync(options) |> Async.AwaitTask
        
        let subscribeOptions = 
            MqttTopicFilterBuilder()
                .WithTopic("sensors/+")
                .Build()
        
        do! mqttClient.SubscribeAsync(subscribeOptions) |> Async.AwaitTask
        client <- Some mqttClient
        printfn "MQTT Subscriber connected and listening..."
    }
    
    member this.Disconnect() =
        match client with
        | Some mqttClient -> mqttClient.DisconnectAsync() |> ignore
        | None -> ()

// ============ gRPC EXAMPLES ============

// Note: In a real implementation, you would generate these from .proto files
// using the protobuf compiler

type StreamRequest = {
    Message: string
    Timestamp: string
}

type StreamResponse = {
    Message: string
    Timestamp: string
    Count: int
}

module StreamRequest =
    let create message =
        {
            Message = message
            Timestamp = DateTime.UtcNow.ToString("O")
        }

module StreamResponse =
    let create message count =
        {
            Message = message
            Timestamp = DateTime.UtcNow.ToString("O")
            Count = count
        }

// Mock gRPC service implementation
type StreamingService() =
    
    member this.ServerStreamAsync(request: StreamRequest) = async {
        printfn "Server streaming request: %s" request.Message
        
        let responses = [
            for i in 0..9 do
                do! Async.Sleep(1000)
                yield StreamResponse.create (sprintf "Stream message %d" i) i
        ]
        
        return responses
    }
    
    member this.BidirectionalStreamAsync(requests: StreamRequest list) = async {
        let responses = [
            for request in requests do
                printfn "Received: %s" request.Message
                yield StreamResponse.create (sprintf "Echo: %s" request.Message) 0
        ]
        
        return responses
    }

type GRPCServer() =
    let mutable server: Server option = None
    
    member this.StartAsync() = async {
        // Note: This is a simplified example
        // In a real implementation, you would use the generated gRPC server code
        
        printfn "gRPC Server running on port 50051"
        
        // Mock server implementation
        let service = StreamingService()
        
        // Keep the server running
        do! Async.Sleep(Timeout.Infinite)
    }
    
    member this.Stop() =
        match server with
        | Some s -> s.ShutdownAsync() |> ignore
        | None -> ()

type GRPCClient() =
    
    member this.ConnectAsync() = async {
        // Note: This is a simplified example
        // In a real implementation, you would use the generated gRPC client code
        
        printfn "gRPC Client connecting to localhost:50051"
    }
    
    member this.ServerStreamAsync() = async {
        printfn "Starting server stream..."
        
        let request = StreamRequest.create "Start streaming"
        
        // Mock server streaming call
        for i in 0..9 do
            printfn "Server stream response: Stream message %d" i
            do! Async.Sleep(1000)
        
        // Real implementation would be:
        // let channel = GrpcChannel.ForAddress("http://localhost:50051")
        // let client = new StreamingService.StreamingServiceClient(channel)
        // let call = client.ServerStream(request)
        // 
        // let! responses = call.ResponseStream.ReadAllAsync() |> Async.AwaitTask
        // for response in responses do
        //     printfn "Server stream response: %s" response.Message
    }
    
    member this.BidirectionalStreamAsync() = async {
        printfn "Starting bidirectional stream..."
        
        // Mock bidirectional streaming
        let requests = [
            for i in 0..4 do
                do! Async.Sleep(2000)
                yield StreamRequest.create (sprintf "Client message %d" i)
        ]
        
        for request in requests do
            printfn "Sending: %s" request.Message
            printfn "Bidirectional response: Echo: %s" request.Message
        
        // Real implementation would be:
        // let channel = GrpcChannel.ForAddress("http://localhost:50051")
        // let client = new StreamingService.StreamingServiceClient(channel)
        // let call = client.BidirectionalStream()
        // 
        // // Send requests
        // for request in requests do
        //     do! call.RequestStream.WriteAsync(request) |> Async.AwaitTask
        // 
        // do! call.RequestStream.CompleteAsync() |> Async.AwaitTask
        // 
        // // Receive responses
        // let! responses = call.ResponseStream.ReadAllAsync() |> Async.AwaitTask
        // for response in responses do
        //     printfn "Bidirectional response: %s" response.Message
    }

// Example usage
[<EntryPoint>]
let main argv =
    printfn "F# MQTT & gRPC streaming examples ready"
    
    // MQTT Examples
    // let publisher = MQTTPublisher("localhost", 1883)
    // Async.Start(async {
    //     do! publisher.ConnectAsync()
    //     do! publisher.PublishSensorDataAsync()
    // })
    
    // let subscriber = MQTTSubscriber("localhost", 1883)
    // Async.Start(async {
    //     do! subscriber.ConnectAsync()
    // })
    
    // gRPC Examples
    // let server = GRPCServer()
    // Async.Start(server.StartAsync())
    
    // Thread.Sleep(1000) // Wait for server to start
    
    // let client = GRPCClient()
    // Async.RunSynchronously(async {
    //     do! client.ConnectAsync()
    //     do! client.ServerStreamAsync()
    //     do! client.BidirectionalStreamAsync()
    // })
    
    0 // Return exit code