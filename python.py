# Python - MQTT & gRPC Streaming Examples
# Dependencies: pip install paho-mqtt grpcio grpcio-tools

import json
import time
import threading
from datetime import datetime
import paho.mqtt.client as mqtt
import grpc
from concurrent import futures

# ============ MQTT EXAMPLES ============

class MQTTPublisher:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.client = mqtt.Client()
        self.client.connect(broker_host, broker_port, 60)
        
    def publish_sensor_data(self):
        """Publish sensor data every 2 seconds"""
        while True:
            message = {
                "timestamp": datetime.now().isoformat(),
                "data": __import__('random').random() * 100,
                "sensor": "temperature"
            }
            
            self.client.publish("sensors/temperature", json.dumps(message))
            print(f"Published: {message}")
            time.sleep(2)

class MQTTSubscriber:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(broker_host, broker_port, 60)
        
    def on_connect(self, client, userdata, flags, rc):
        print(f"MQTT Subscriber connected with result code {rc}")
        client.subscribe("sensors/+")
        
    def on_message(self, client, userdata, msg):
        topic = msg.topic
        message = json.loads(msg.payload.decode())
        print(f"Received on {topic}: {message}")
        
    def start_listening(self):
        self.client.loop_forever()

# ============ gRPC EXAMPLES ============

# Note: In a real implementation, you would generate these from .proto files
# using: python -m grpc_tools.protoc --python_out=. --grpc_python_out=. streaming.proto

class StreamingServicer:
    """gRPC Server implementation"""
    
    def ServerStream(self, request, context):
        """Server streaming - send multiple responses"""
        print(f"Server streaming request: {request}")
        
        for i in range(10):
            response = {
                "message": f"Stream message {i}",
                "timestamp": datetime.now().isoformat(),
                "count": i
            }
            yield response
            time.sleep(1)
    
    def BidirectionalStream(self, request_iterator, context):
        """Bidirectional streaming"""
        for request in request_iterator:
            print(f"Received: {request}")
            response = {
                "message": f"Echo: {request.get('message', '')}",
                "timestamp": datetime.now().isoformat()
            }
            yield response

def run_grpc_server():
    """Start gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # server.add_StreamingServiceServicer_to_server(StreamingServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("gRPC Server running on port 50051")
    
    try:
        while True:
            time.sleep(86400)  # Keep server running
    except KeyboardInterrupt:
        server.stop(0)

def run_grpc_client():
    """gRPC Client with streaming"""
    with grpc.insecure_channel('localhost:50051') as channel:
        # stub = StreamingServiceStub(channel)
        
        # Server streaming call
        print("Starting server stream...")
        # responses = stub.ServerStream({"message": "Start streaming"})
        # for response in responses:
        #     print(f"Server stream response: {response}")
        
        # Bidirectional streaming
        def generate_requests():
            for i in range(5):
                yield {
                    "message": f"Client message {i}",
                    "timestamp": datetime.now().isoformat()
                }
                time.sleep(2)
        
        # responses = stub.BidirectionalStream(generate_requests())
        # for response in responses:
        #     print(f"Bidirectional response: {response}")

# Example usage
if __name__ == "__main__":
    # MQTT Examples
    # publisher = MQTTPublisher()
    # threading.Thread(target=publisher.publish_sensor_data, daemon=True).start()
    
    # subscriber = MQTTSubscriber()
    # threading.Thread(target=subscriber.start_listening, daemon=True).start()
    
    # gRPC Examples
    # threading.Thread(target=run_grpc_server, daemon=True).start()
    # time.sleep(1)  # Wait for server to start
    # run_grpc_client()
    
    print("Python MQTT & gRPC streaming examples ready")