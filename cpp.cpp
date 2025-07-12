// C++ - MQTT & gRPC Streaming Examples
// Dependencies: paho-mqtt-cpp, grpc++, protobuf

#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <random>
#include <json/json.h>
#include <mqtt/async_client.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

using namespace std;
using namespace std::chrono;

// ============ MQTT EXAMPLES ============

class SensorData {
public:
    string timestamp;
    double data;
    string sensor;
    
    Json::Value toJson() const {
        Json::Value json;
        json["timestamp"] = timestamp;
        json["data"] = data;
        json["sensor"] = sensor;
        return json;
    }
    
    static SensorData fromJson(const Json::Value& json) {
        SensorData data;
        data.timestamp = json["timestamp"].asString();
        data.data = json["data"].asDouble();
        data.sensor = json["sensor"].asString();
        return data;
    }
};

class MQTTPublisher {
private:
    mqtt::async_client client;
    random_device rd;
    mt19937 gen;
    uniform_real_distribution<> dis;
    
public:
    MQTTPublisher(const string& brokerUrl) 
        : client(brokerUrl, "CppPublisher"), gen(rd()), dis(0.0, 100.0) {
        
        mqtt::connect_options connOpts;
        connOpts.set_keep_alive_interval(20);
        connOpts.set_clean_session(true);
        
        try {
            client.connect(connOpts)->wait();
            cout << "MQTT Publisher connected" << endl;
        } catch (const mqtt::exception& exc) {
            cerr << "Error connecting: " << exc.what() << endl;
        }
    }
    
    void publishSensorData() {
        while (true) {
            auto now = system_clock::now();
            auto time_t = system_clock::to_time_t(now);
            
            SensorData data;
            data.timestamp = to_string(time_t);
            data.data = dis(gen);
            data.sensor = "temperature";
            
            Json::StreamWriterBuilder builder;
            string jsonString = Json::writeString(builder, data.toJson());
            
            mqtt::message_ptr pubmsg = mqtt::make_message("sensors/temperature", jsonString);
            pubmsg->set_qos(1);
            
            try {
                client.publish(pubmsg)->wait();
                cout << "Published: " << jsonString << endl;
            } catch (const mqtt::exception& exc) {
                cerr << "Error publishing: " << exc.what() << endl;
            }
            
            this_thread::sleep_for(seconds(2));
        }
    }
};

class MQTTSubscriber : public virtual mqtt::callback {
private:
    mqtt::async_client client;
    
public:
    MQTTSubscriber(const string& brokerUrl) : client(brokerUrl, "CppSubscriber") {
        client.set_callback(*this);
        
        mqtt::connect_options connOpts;
        connOpts.set_keep_alive_interval(20);
        connOpts.set_clean_session(true);
        
        try {
            client.connect(connOpts)->wait();
            client.subscribe("sensors/+", 1)->wait();
            cout << "MQTT Subscriber connected and listening..." << endl;
        } catch (const mqtt::exception& exc) {
            cerr << "Error connecting: " << exc.what() << endl;
        }
    }
    
    void message_arrived(mqtt::const_message_ptr msg) override {
        Json::Reader reader;
        Json::Value json;
        
        if (reader.parse(msg->get_payload_str(), json)) {
            SensorData data = SensorData::fromJson(json);
            cout << "Received on " << msg->get_topic() << ": " 
                 << "timestamp=" << data.timestamp 
                 << ", data=" << data.data 
                 << ", sensor=" << data.sensor << endl;
        }
    }
    
    void connection_lost(const string& cause) override {
        cout << "Connection lost: " << cause << endl;
    }
    
    void delivery_complete(mqtt::delivery_token_ptr token) override {
        // Not used for subscriber
    }
};

// ============ gRPC EXAMPLES ============

// Note: In a real implementation, you would generate these from .proto files
// using the protobuf compiler

struct StreamRequest {
    string message;
    string timestamp;
};

struct StreamResponse {
    string message;
    string timestamp;
    int32_t count;
};

// Mock gRPC service implementation
class StreamingServiceImpl final {
public:
    grpc::Status ServerStream(grpc::ServerContext* context,
                             const StreamRequest* request,
                             grpc::ServerWriter<StreamResponse>* writer) {
        cout << "Server streaming request: " << request->message << endl;
        
        for (int i = 0; i < 10; i++) {
            StreamResponse response;
            response.message = "Stream message " + to_string(i);
            response.timestamp = to_string(system_clock::to_time_t(system_clock::now()));
            response.count = i;
            
            writer->Write(response);
            this_thread::sleep_for(seconds(1));
        }
        
        return grpc::Status::OK;
    }
    
    grpc::Status BidirectionalStream(grpc::ServerContext* context,
                                   grpc::ServerReaderWriter<StreamResponse, StreamRequest>* stream) {
        StreamRequest request;
        while (stream->Read(&request)) {
            cout << "Received: " << request.message << endl;
            
            StreamResponse response;
            response.message = "Echo: " + request.message;
            response.timestamp = to_string(system_clock::to_time_t(system_clock::now()));
            response.count = 0;
            
            stream->Write(response);
        }
        
        return grpc::Status::OK;
    }
};

void runGRPCServer() {
    string server_address("0.0.0.0:50051");
    StreamingServiceImpl service;
    
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    
    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // builder.RegisterService(&service);
    
    unique_ptr<grpc::Server> server(builder.BuildAndStart());
    cout << "gRPC Server running on port 50051" << endl;
    
    server->Wait();
}

void runGRPCClient() {
    auto channel = grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials());
    // auto stub = StreamingService::NewStub(channel);
    
    grpc::ClientContext context;
    
    // Server streaming call
    cout << "Starting server stream..." << endl;
    StreamRequest request;
    request.message = "Start streaming";
    request.timestamp = to_string(system_clock::to_time_t(system_clock::now()));
    
    // unique_ptr<grpc::ClientReader<StreamResponse>> reader(
    //     stub->ServerStream(&context, request));
    
    // StreamResponse response;
    // while (reader->Read(&response)) {
    //     cout << "Server stream response: " << response.message << endl;
    // }
    
    // grpc::Status status = reader->Finish();
    
    // Bidirectional streaming
    cout << "Starting bidirectional stream..." << endl;
    grpc::ClientContext bi_context;
    // shared_ptr<grpc::ClientReaderWriter<StreamRequest, StreamResponse>> stream(
    //     stub->BidirectionalStream(&bi_context));
    
    // Send messages in a separate thread
    thread writer([&stream]() {
        for (int i = 0; i < 5; i++) {
            StreamRequest req;
            req.message = "Client message " + to_string(i);
            req.timestamp = to_string(system_clock::to_time_t(system_clock::now()));
            
            // stream->Write(req);
            this_thread::sleep_for(seconds(2));
        }
        // stream->WritesDone();
    });
    
    // Read responses
    // StreamResponse resp;
    // while (stream->Read(&resp)) {
    //     cout << "Bidirectional response: " << resp.message << endl;
    // }
    
    writer.join();
    // stream->Finish();
}

// Example usage
int main() {
    cout << "C++ MQTT & gRPC streaming examples ready" << endl;
    
    // MQTT Examples
    // MQTTPublisher publisher("tcp://localhost:1883");
    // thread publisherThread(&MQTTPublisher::publishSensorData, &publisher);
    
    // MQTTSubscriber subscriber("tcp://localhost:1883");
    
    // gRPC Examples
    // thread serverThread(runGRPCServer);
    // this_thread::sleep_for(seconds(1)); // Wait for server to start
    // runGRPCClient();
    
    // publisherThread.join();
    // serverThread.join();
    
    return 0;
}