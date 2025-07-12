# Ruby - MQTT & gRPC Streaming Examples
# Dependencies: gem install mqtt grpc grpc-tools

require 'mqtt'
require 'grpc'
require 'json'
require 'time'
require 'thread'

# ============ MQTT EXAMPLES ============

class SensorData
  attr_accessor :timestamp, :data, :sensor
  
  def initialize(timestamp, data, sensor)
    @timestamp = timestamp
    @data = data
    @sensor = sensor
  end
  
  def to_json(*args)
    {
      timestamp: @timestamp,
      data: @data,
      sensor: @sensor
    }.to_json(*args)
  end
  
  def self.from_json(json_str)
    data = JSON.parse(json_str)
    new(data['timestamp'], data['data'], data['sensor'])
  end
end

class MQTTPublisher
  def initialize(broker_host = 'localhost', broker_port = 1883)
    @client = MQTT::Client.connect(
      host: broker_host,
      port: broker_port,
      client_id: 'RubyPublisher'
    )
    puts "MQTT Publisher connected"
  end
  
  def publish_sensor_data
    loop do
      data = SensorData.new(
        Time.now.iso8601,
        rand(0.0..100.0),
        'temperature'
      )
      
      @client.publish('sensors/temperature', data.to_json, retain: false, qos: 1)
      puts "Published: #{data.to_json}"
      
      sleep(2)
    end
  end
  
  def disconnect
    @client.disconnect
  end
end

class MQTTSubscriber
  def initialize(broker_host = 'localhost', broker_port = 1883)
    @client = MQTT::Client.connect(
      host: broker_host,
      port: broker_port,
      client_id: 'RubySubscriber'
    )
    puts "MQTT Subscriber connected"
  end
  
  def subscribe
    @client.subscribe('sensors/+')
    puts "MQTT Subscriber listening..."
    
    @client.get do |topic, message|
      begin
        data = SensorData.from_json(message)
        puts "Received on #{topic}: timestamp=#{data.timestamp}, data=#{data.data}, sensor=#{data.sensor}"
      rescue JSON::ParserError => e
        puts "Error parsing message: #{e.message}"
      end
    end
  end
  
  def disconnect
    @client.disconnect
  end
end

# ============ gRPC EXAMPLES ============

# Note: In a real implementation, you would generate these from .proto files
# using: grpc_tools_ruby_protoc --ruby_out=. --grpc_out=. streaming.proto

class StreamRequest
  attr_accessor :message, :timestamp
  
  def initialize(message, timestamp)
    @message = message
    @timestamp = timestamp
  end
end

class StreamResponse
  attr_accessor :message, :timestamp, :count
  
  def initialize(message, timestamp, count = 0)
    @message = message
    @timestamp = timestamp
    @count = count
  end
end

# Mock gRPC service implementation
class StreamingServiceImpl
  def server_stream(request, _call)
    puts "Server streaming request: #{request.message}"
    
    Enumerator.new do |yielder|
      10.times do |i|
        response = StreamResponse.new(
          "Stream message #{i}",
          Time.now.iso8601,
          i
        )
        
        yielder << response
        sleep(1)
      end
    end
  end
  
  def bidirectional_stream(requests)
    Enumerator.new do |yielder|
      requests.each do |request|
        puts "Received: #{request.message}"
        
        response = StreamResponse.new(
          "Echo: #{request.message}",
          Time.now.iso8601
        )
        
        yielder << response
      end
    end
  end
end

class GRPCServer
  def start
    # Note: This is a simplified example
    # In a real implementation, you would use the generated gRPC server code
    
    puts "gRPC Server running on port 50051"
    
    server = GRPC::RpcServer.new
    server.add_http2_port('0.0.0.0:50051', :this_port_is_insecure)
    # server.handle(StreamingServiceImpl.new)
    
    # Mock server loop
    Thread.new do
      loop do
        sleep(1)
      end
    end
    
    server.run_till_terminated_or_interrupted([1, 'int', 'SIGQUIT'])
  end
end

class GRPCClient
  def initialize
    # Note: This is a simplified example
    # In a real implementation, you would use the generated gRPC client code
    
    puts "gRPC Client connecting to localhost:50051"
    # @stub = StreamingService::Stub.new('localhost:50051', :this_channel_is_insecure)
  end
  
  def server_stream
    puts "Starting server stream..."
    
    request = StreamRequest.new("Start streaming", Time.now.iso8601)
    
    # Mock server streaming call
    10.times do |i|
      puts "Server stream response: Stream message #{i}"
      sleep(1)
    end
    
    # Real implementation would be:
    # responses = @stub.server_stream(request)
    # responses.each do |response|
    #   puts "Server stream response: #{response.message}"
    # end
  end
  
  def bidirectional_stream
    puts "Starting bidirectional stream..."
    
    # Mock bidirectional streaming
    requests = Enumerator.new do |yielder|
      5.times do |i|
        request = StreamRequest.new("Client message #{i}", Time.now.iso8601)
        yielder << request
        sleep(2)
      end
    end
    
    requests.each do |request|
      puts "Sending: #{request.message}"
      puts "Bidirectional response: Echo: #{request.message}"
    end
    
    # Real implementation would be:
    # responses = @stub.bidirectional_stream(requests)
    # responses.each do |response|
    #   puts "Bidirectional response: #{response.message}"
    # end
  end
end

# Example usage
def main
  puts "Ruby MQTT & gRPC streaming examples ready"
  
  # MQTT Examples
  # publisher = MQTTPublisher.new
  # publisher_thread = Thread.new { publisher.publish_sensor_data }
  
  # subscriber = MQTTSubscriber.new
  # subscriber_thread = Thread.new { subscriber.subscribe }
  
  # gRPC Examples
  # server = GRPCServer.new
  # server_thread = Thread.new { server.start }
  
  # sleep(1) # Wait for server to start
  
  # client = GRPCClient.new
  # client.server_stream
  # client.bidirectional_stream
  
  # Clean up
  # publisher.disconnect
  # subscriber.disconnect
  # publisher_thread.kill
  # subscriber_thread.kill
  # server_thread.kill
end

if __FILE__ == $0
  main
end