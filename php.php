<?php
// PHP - MQTT & gRPC Streaming Examples
// Dependencies: composer require php-mqtt/client grpc/grpc

require_once 'vendor/autoload.php';

use PhpMqtt\Client\MqttClient;
use PhpMqtt\Client\ConnectionSettings;
use Grpc\ChannelCredentials;

// ============ MQTT EXAMPLES ============

class SensorData {
    public $timestamp;
    public $data;
    public $sensor;
    
    public function __construct($timestamp, $data, $sensor) {
        $this->timestamp = $timestamp;
        $this->data = $data;
        $this->sensor = $sensor;
    }
    
    public function toJson() {
        return json_encode([
            'timestamp' => $this->timestamp,
            'data' => $this->data,
            'sensor' => $this->sensor
        ]);
    }
    
    public static function fromJson($json) {
        $data = json_decode($json, true);
        return new self($data['timestamp'], $data['data'], $data['sensor']);
    }
}

class MQTTPublisher {
    private $client;
    
    public function __construct($brokerHost = 'localhost', $brokerPort = 1883) {
        $connectionSettings = new ConnectionSettings();
        $connectionSettings->setKeepAliveInterval(20);
        
        $this->client = new MqttClient($brokerHost, $brokerPort, 'PhpPublisher');
        $this->client->connect($connectionSettings);
        
        echo "MQTT Publisher connected\n";
    }
    
    public function publishSensorData() {
        while (true) {
            $data = new SensorData(
                date('c'),
                mt_rand(0, 10000) / 100.0,
                'temperature'
            );
            
            $this->client->publish(
                'sensors/temperature',
                $data->toJson(),
                1
            );
            
            echo "Published: " . $data->toJson() . "\n";
            sleep(2);
        }
    }
    
    public function __destruct() {
        $this->client->disconnect();
    }
}

class MQTTSubscriber {
    private $client;
    
    public function __construct($brokerHost = 'localhost', $brokerPort = 1883) {
        $connectionSettings = new ConnectionSettings();
        $connectionSettings->setKeepAliveInterval(20);
        
        $this->client = new MqttClient($brokerHost, $brokerPort, 'PhpSubscriber');
        $this->client->connect($connectionSettings);
        
        echo "MQTT Subscriber connected\n";
    }
    
    public function subscribe() {
        $this->client->subscribe('sensors/+', function ($topic, $message) {
            $data = SensorData::fromJson($message);
            echo "Received on {$topic}: timestamp={$data->timestamp}, data={$data->data}, sensor={$data->sensor}\n";
        }, 1);
        
        echo "MQTT Subscriber listening...\n";
        $this->client->loop(true);
    }
    
    public function __destruct() {
        $this->client->disconnect();
    }
}

// ============ gRPC EXAMPLES ============

// Note: In a real implementation, you would generate these from .proto files
// using the protobuf compiler

class StreamRequest {
    public $message;
    public $timestamp;
    
    public function __construct($message, $timestamp) {
        $this->message = $message;
        $this->timestamp = $timestamp;
    }
}

class StreamResponse {
    public $message;
    public $timestamp;
    public $count;
    
    public function __construct($message, $timestamp, $count = 0) {
        $this->message = $message;
        $this->timestamp = $timestamp;
        $this->count = $count;
    }
}

// Mock gRPC service implementation
class StreamingServiceImpl {
    
    public function ServerStream($request) {
        echo "Server streaming request: {$request->message}\n";
        
        for ($i = 0; $i < 10; $i++) {
            $response = new StreamResponse(
                "Stream message {$i}",
                date('c'),
                $i
            );
            
            // In a real implementation, this would yield the response
            echo "Server stream response: {$response->message}\n";
            sleep(1);
        }
    }
    
    public function BidirectionalStream($requestIterator) {
        foreach ($requestIterator as $request) {
            echo "Received: {$request->message}\n";
            
            $response = new StreamResponse(
                "Echo: {$request->message}",
                date('c')
            );
            
            // In a real implementation, this would yield the response
            echo "Bidirectional response: {$response->message}\n";
        }
    }
}

class GRPCServer {
    private $server;
    
    public function start() {
        // Note: This is a simplified example
        // In a real implementation, you would use the generated gRPC server code
        
        echo "gRPC Server running on port 50051\n";
        
        $service = new StreamingServiceImpl();
        
        // Mock server loop
        while (true) {
            // Handle incoming requests
            sleep(1);
        }
    }
}

class GRPCClient {
    private $client;
    
    public function __construct() {
        // Note: This is a simplified example
        // In a real implementation, you would use the generated gRPC client code
        
        echo "gRPC Client connecting to localhost:50051\n";
    }
    
    public function serverStream() {
        echo "Starting server stream...\n";
        
        $request = new StreamRequest("Start streaming", date('c'));
        
        // Mock server streaming call
        for ($i = 0; $i < 10; $i++) {
            echo "Server stream response: Stream message {$i}\n";
            sleep(1);
        }
    }
    
    public function bidirectionalStream() {
        echo "Starting bidirectional stream...\n";
        
        // Mock bidirectional streaming
        for ($i = 0; $i < 5; $i++) {
            $request = new StreamRequest("Client message {$i}", date('c'));
            echo "Sending: {$request->message}\n";
            echo "Bidirectional response: Echo: {$request->message}\n";
            sleep(2);
        }
    }
}

// Example usage
function main() {
    echo "PHP MQTT & gRPC streaming examples ready\n";
    
    // MQTT Examples
    // $publisher = new MQTTPublisher();
    // 
    // // Run publisher in background (in real app, use process forking)
    // if (pcntl_fork() == 0) {
    //     $publisher->publishSensorData();
    //     exit;
    // }
    // 
    // $subscriber = new MQTTSubscriber();
    // 
    // // Run subscriber in background
    // if (pcntl_fork() == 0) {
    //     $subscriber->subscribe();
    //     exit;
    // }
    
    // gRPC Examples
    // 
    // // Run server in background
    // if (pcntl_fork() == 0) {
    //     $server = new GRPCServer();
    //     $server->start();
    //     exit;
    // }
    // 
    // sleep(1); // Wait for server to start
    // 
    // $client = new GRPCClient();
    // $client->serverStream();
    // $client->bidirectionalStream();
    
    // Wait for child processes
    // while (pcntl_waitpid(0, $status) != -1) {
    //     // Wait for all child processes to finish
    // }
}

if (php_sapi_name() === 'cli') {
    main();
}
?>