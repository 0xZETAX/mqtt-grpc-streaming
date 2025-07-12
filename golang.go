// Go - MQTT & gRPC Streaming Examples
// Dependencies: go get github.com/eclipse/paho.mqtt.golang google.golang.org/grpc

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ============ MQTT EXAMPLES ============

type SensorData struct {
	Timestamp string  `json:"timestamp"`
	Data      float64 `json:"data"`
	Sensor    string  `json:"sensor"`
}

type MQTTPublisher struct {
	client mqtt.Client
}

func NewMQTTPublisher(brokerURL string) *MQTTPublisher {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID("GoPublisher")

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	return &MQTTPublisher{client: client}
}

func (p *MQTTPublisher) PublishSensorData() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		data := SensorData{
			Timestamp: time.Now().Format(time.RFC3339),
			Data:      rand.Float64() * 100,
			Sensor:    "temperature",
		}

		payload, _ := json.Marshal(data)
		token := p.client.Publish("sensors/temperature", 1, false, payload)
		token.Wait()

		fmt.Printf("Published: %+v\n", data)
	}
}

type MQTTSubscriber struct {
	client mqtt.Client
}

func NewMQTTSubscriber(brokerURL string) *MQTTSubscriber {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID("GoSubscriber")

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	return &MQTTSubscriber{client: client}
}

func (s *MQTTSubscriber) Subscribe() {
	token := s.client.Subscribe("sensors/+", 1, func(client mqtt.Client, msg mqtt.Message) {
		var data SensorData
		json.Unmarshal(msg.Payload(), &data)
		fmt.Printf("Received on %s: %+v\n", msg.Topic(), data)
	})
	token.Wait()

	fmt.Println("MQTT Subscriber connected and listening...")
}

// ============ gRPC EXAMPLES ============

// Note: In a real implementation, you would generate these from .proto files
// using: protoc --go_out=. --go-grpc_out=. streaming.proto

type StreamRequest struct {
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

type StreamResponse struct {
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
	Count     int32  `json:"count"`
}

// Mock gRPC service interface
type StreamingServiceServer interface {
	ServerStream(req *StreamRequest, stream StreamingService_ServerStreamServer) error
	BidirectionalStream(stream StreamingService_BidirectionalStreamServer) error
}

type StreamingService_ServerStreamServer interface {
	Send(*StreamResponse) error
}

type StreamingService_BidirectionalStreamServer interface {
	Send(*StreamResponse) error
	Recv() (*StreamRequest, error)
}

type streamingServiceImpl struct{}

func (s *streamingServiceImpl) ServerStream(req *StreamRequest, stream StreamingService_ServerStreamServer) error {
	fmt.Printf("Server streaming request: %+v\n", req)

	for i := 0; i < 10; i++ {
		response := &StreamResponse{
			Message:   fmt.Sprintf("Stream message %d", i),
			Timestamp: time.Now().Format(time.RFC3339),
			Count:     int32(i),
		}

		if err := stream.Send(response); err != nil {
			return err
		}

		time.Sleep(1 * time.Second)
	}

	return nil
}

func (s *streamingServiceImpl) BidirectionalStream(stream StreamingService_BidirectionalStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		fmt.Printf("Received: %+v\n", req)

		response := &StreamResponse{
			Message:   fmt.Sprintf("Echo: %s", req.Message),
			Timestamp: time.Now().Format(time.RFC3339),
		}

		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func runGRPCServer() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	server := grpc.NewServer()
	// RegisterStreamingServiceServer(server, &streamingServiceImpl{})

	fmt.Println("gRPC Server running on port 50051")
	if err := server.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func runGRPCClient() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// client := NewStreamingServiceClient(conn)
	ctx := context.Background()

	// Server streaming call
	fmt.Println("Starting server stream...")
	// stream, err := client.ServerStream(ctx, &StreamRequest{Message: "Start streaming"})
	// if err != nil {
	//     log.Fatalf("ServerStream failed: %v", err)
	// }

	// for {
	//     response, err := stream.Recv()
	//     if err != nil {
	//         break
	//     }
	//     fmt.Printf("Server stream response: %+v\n", response)
	// }

	// Bidirectional streaming
	fmt.Println("Starting bidirectional stream...")
	// biStream, err := client.BidirectionalStream(ctx)
	// if err != nil {
	//     log.Fatalf("BidirectionalStream failed: %v", err)
	// }

	// Send messages
	go func() {
		for i := 0; i < 5; i++ {
			req := &StreamRequest{
				Message:   fmt.Sprintf("Client message %d", i),
				Timestamp: time.Now().Format(time.RFC3339),
			}
			// biStream.Send(req)
			time.Sleep(2 * time.Second)
		}
		// biStream.CloseSend()
	}()

	// Receive responses
	// for {
	//     response, err := biStream.Recv()
	//     if err != nil {
	//         break
	//     }
	//     fmt.Printf("Bidirectional response: %+v\n", response)
	// }
}

func main() {
	fmt.Println("Go MQTT & gRPC streaming examples ready")

	// MQTT Examples
	// publisher := NewMQTTPublisher("tcp://localhost:1883")
	// go publisher.PublishSensorData()

	// subscriber := NewMQTTSubscriber("tcp://localhost:1883")
	// go subscriber.Subscribe()

	// gRPC Examples
	// go runGRPCServer()
	// time.Sleep(1 * time.Second) // Wait for server to start
	// runGRPCClient()

	// Keep the program running
	select {}
}