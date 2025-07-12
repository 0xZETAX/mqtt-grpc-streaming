-- Lua - MQTT & gRPC Streaming Examples
-- Dependencies: luarocks install lua-resty-mqtt, luarocks install grpc

local json = require("cjson")
local socket = require("socket")

-- ============ MQTT EXAMPLES ============

local SensorData = {}
SensorData.__index = SensorData

function SensorData:new(data, sensor)
    local obj = {
        timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        data = data or 0,
        sensor = sensor or "temperature"
    }
    setmetatable(obj, self)
    return obj
end

function SensorData:toJson()
    return json.encode({
        timestamp = self.timestamp,
        data = self.data,
        sensor = self.sensor
    })
end

function SensorData.fromJson(jsonStr)
    local success, data = pcall(json.decode, jsonStr)
    if success then
        return SensorData:new(data.data, data.sensor)
    else
        print("Error parsing JSON: " .. tostring(data))
        return nil
    end
end

local MQTTPublisher = {}
MQTTPublisher.__index = MQTTPublisher

function MQTTPublisher:new(brokerHost, brokerPort)
    local obj = {
        brokerHost = brokerHost or "localhost",
        brokerPort = brokerPort or 1883,
        connected = false
    }
    setmetatable(obj, self)
    return obj
end

function MQTTPublisher:connect()
    -- Note: This is a simplified example
    -- In a real implementation, you would use a proper MQTT library
    
    print("MQTT Publisher connected to " .. self.brokerHost .. ":" .. self.brokerPort)
    self.connected = true
end

function MQTTPublisher:publishSensorData()
    if not self.connected then
        print("Publisher not connected")
        return
    end
    
    while true do
        local data = SensorData:new(math.random() * 100, "temperature")
        local jsonData = data:toJson()
        
        -- Mock publish
        print("Published: " .. jsonData)
        
        -- In a real implementation, you would use:
        -- self.client:publish("sensors/temperature", jsonData, 1, false)
        
        socket.sleep(2)
    end
end

function MQTTPublisher:disconnect()
    self.connected = false
    print("MQTT Publisher disconnected")
end

local MQTTSubscriber = {}
MQTTSubscriber.__index = MQTTSubscriber

function MQTTSubscriber:new(brokerHost, brokerPort)
    local obj = {
        brokerHost = brokerHost or "localhost",
        brokerPort = brokerPort or 1883,
        connected = false
    }
    setmetatable(obj, self)
    return obj
end

function MQTTSubscriber:connect()
    -- Note: This is a simplified example
    -- In a real implementation, you would use a proper MQTT library
    
    print("MQTT Subscriber connected to " .. self.brokerHost .. ":" .. self.brokerPort)
    print("MQTT Subscriber listening...")
    self.connected = true
end

function MQTTSubscriber:onMessage(topic, message)
    local data = SensorData.fromJson(message)
    if data then
        print(string.format("Received on %s: timestamp=%s, data=%s, sensor=%s",
                          topic, data.timestamp, data.data, data.sensor))
    end
end

function MQTTSubscriber:subscribe()
    if not self.connected then
        print("Subscriber not connected")
        return
    end
    
    -- Mock subscription
    -- In a real implementation, you would use:
    -- self.client:subscribe("sensors/+", 1)
    -- self.client:on_message(function(topic, payload)
    --     self:onMessage(topic, payload)
    -- end)
end

function MQTTSubscriber:disconnect()
    self.connected = false
    print("MQTT Subscriber disconnected")
end

-- ============ gRPC EXAMPLES ============

-- Note: In a real implementation, you would generate these from .proto files
-- using the protobuf compiler

local StreamRequest = {}
StreamRequest.__index = StreamRequest

function StreamRequest:new(message)
    local obj = {
        message = message or "",
        timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ")
    }
    setmetatable(obj, self)
    return obj
end

local StreamResponse = {}
StreamResponse.__index = StreamResponse

function StreamResponse:new(message, count)
    local obj = {
        message = message or "",
        timestamp = os.date("!%Y-%m-%dT%H:%M:%SZ"),
        count = count or 0
    }
    setmetatable(obj, self)
    return obj
end

-- Mock gRPC service implementation
local StreamingService = {}
StreamingService.__index = StreamingService

function StreamingService:new()
    local obj = {}
    setmetatable(obj, self)
    return obj
end

function StreamingService:serverStream(request)
    print("Server streaming request: " .. request.message)
    
    local responses = {}
    for i = 0, 9 do
        socket.sleep(1)
        local response = StreamResponse:new("Stream message " .. i, i)
        table.insert(responses, response)
    end
    
    return responses
end

function StreamingService:bidirectionalStream(requests)
    local responses = {}
    
    for _, request in ipairs(requests) do
        print("Received: " .. request.message)
        local response = StreamResponse:new("Echo: " .. request.message, 0)
        table.insert(responses, response)
    end
    
    return responses
end

local GRPCServer = {}
GRPCServer.__index = GRPCServer

function GRPCServer:new()
    local obj = {
        service = StreamingService:new(),
        running = false
    }
    setmetatable(obj, self)
    return obj
end

function GRPCServer:start()
    -- Note: This is a simplified example
    -- In a real implementation, you would use a proper gRPC library
    
    print("gRPC Server running on port 50051")
    self.running = true
    
    -- Mock server loop
    while self.running do
        socket.sleep(1)
    end
end

function GRPCServer:stop()
    self.running = false
    print("gRPC Server stopped")
end

local GRPCClient = {}
GRPCClient.__index = GRPCClient

function GRPCClient:new()
    local obj = {}
    setmetatable(obj, self)
    return obj
end

function GRPCClient:connect()
    -- Note: This is a simplified example
    -- In a real implementation, you would use a proper gRPC library
    
    print("gRPC Client connecting to localhost:50051")
end

function GRPCClient:serverStream()
    print("Starting server stream...")
    
    local request = StreamRequest:new("Start streaming")
    
    -- Mock server streaming call
    for i = 0, 9 do
        print("Server stream response: Stream message " .. i)
        socket.sleep(1)
    end
    
    -- Real implementation would be:
    -- local responses = self.stub:serverStream(request)
    -- for _, response in ipairs(responses) do
    --     print("Server stream response: " .. response.message)
    -- end
end

function GRPCClient:bidirectionalStream()
    print("Starting bidirectional stream...")
    
    -- Mock bidirectional streaming
    local requests = {}
    for i = 0, 4 do
        socket.sleep(2)
        local request = StreamRequest:new("Client message " .. i)
        table.insert(requests, request)
    end
    
    for _, request in ipairs(requests) do
        print("Sending: " .. request.message)
        print("Bidirectional response: Echo: " .. request.message)
    end
    
    -- Real implementation would be:
    -- local responses = self.stub:bidirectionalStream(requests)
    -- for _, response in ipairs(responses) do
    --     print("Bidirectional response: " .. response.message)
    -- end
end

-- Example usage
local function main()
    print("Lua MQTT & gRPC streaming examples ready")
    
    -- MQTT Examples
    -- local publisher = MQTTPublisher:new("localhost", 1883)
    -- publisher:connect()
    -- 
    -- -- Run publisher in a coroutine
    -- coroutine.create(function()
    --     publisher:publishSensorData()
    -- end)
    -- 
    -- local subscriber = MQTTSubscriber:new("localhost", 1883)
    -- subscriber:connect()
    -- subscriber:subscribe()
    
    -- gRPC Examples
    -- local server = GRPCServer:new()
    -- 
    -- -- Run server in a coroutine
    -- coroutine.create(function()
    --     server:start()
    -- end)
    -- 
    -- socket.sleep(1) -- Wait for server to start
    -- 
    -- local client = GRPCClient:new()
    -- client:connect()
    -- client:serverStream()
    -- client:bidirectionalStream()
    
    -- Clean up
    -- publisher:disconnect()
    -- subscriber:disconnect()
    -- server:stop()
end

-- Run if this file is executed directly
if arg and arg[0] == "lua.lua" then
    main()
end

-- Export modules for use in other files
return {
    SensorData = SensorData,
    MQTTPublisher = MQTTPublisher,
    MQTTSubscriber = MQTTSubscriber,
    StreamingService = StreamingService,
    GRPCServer = GRPCServer,
    GRPCClient = GRPCClient
}