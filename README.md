# MQTT & gRPC Streaming Examples

This repository contains examples of MQTT and gRPC streaming implementations in 20 popular backend programming languages.

## What is MQTT?

MQTT (Message Queuing Telemetry Transport) is a lightweight, publish-subscribe messaging protocol designed for constrained devices and low-bandwidth, high-latency networks. It's perfect for IoT applications, real-time messaging, and distributed systems.

### Key Features:
- **Lightweight**: Minimal overhead for efficient communication
- **Publish/Subscribe**: Decoupled communication pattern
- **Quality of Service**: Three levels (0, 1, 2) for message delivery guarantees
- **Retained Messages**: Last message on a topic is stored for new subscribers
- **Last Will and Testament**: Automatic notification when clients disconnect unexpectedly

## What is gRPC?

gRPC (Google Remote Procedure Call) is a high-performance, open-source RPC framework that uses HTTP/2 for transport and Protocol Buffers as the interface description language.

### Key Features:
- **High Performance**: Built on HTTP/2 with binary serialization
- **Streaming**: Supports client, server, and bidirectional streaming
- **Language Agnostic**: Code generation for multiple languages
- **Type Safety**: Strong typing with Protocol Buffers
- **Load Balancing**: Built-in load balancing and service discovery

## Streaming Types

### MQTT Streaming
- **Publisher**: Sends messages to topics
- **Subscriber**: Receives messages from subscribed topics
- **Broker**: Routes messages between publishers and subscribers

### gRPC Streaming
- **Unary**: Single request, single response
- **Server Streaming**: Single request, stream of responses
- **Client Streaming**: Stream of requests, single response
- **Bidirectional Streaming**: Stream of requests and responses

## Languages Included

1. **JavaScript/Node.js** - `javascript.js`
2. **Python** - `python.py`
3. **Java** - `java.java`
4. **Go** - `golang.go`
5. **C#** - `csharp.cs`
6. **Rust** - `rust.rs`
7. **C++** - `cpp.cpp`
8. **PHP** - `php.php`
9. **Ruby** - `ruby.rb`
10. **Kotlin** - `kotlin.kt`
11. **Swift** - `swift.swift`
12. **Scala** - `scala.scala`
13. **Dart** - `dart.dart`
14. **TypeScript** - `typescript.ts`
15. **Elixir** - `elixir.ex`
16. **Haskell** - `haskell.hs`
17. **Clojure** - `clojure.clj`
18. **F#** - `fsharp.fs`
19. **Erlang** - `erlang.erl`
20. **Lua** - `lua.lua`

## Getting Started

Each file contains:
- MQTT publisher and subscriber examples
- gRPC server and client with streaming examples
- Basic setup and dependency information
- Comments explaining the implementation

## Common Use Cases

### MQTT
- IoT device communication
- Real-time chat applications
- Live data feeds
- Notification systems
- Sensor data collection

### gRPC
- Microservices communication
- Real-time data streaming
- API development
- Inter-service communication
- High-performance applications

## Installation Notes

Each language example includes comments about required dependencies. Make sure to install the appropriate MQTT and gRPC libraries for your chosen language before running the examples.

## Contributing

Feel free to contribute improvements, additional examples, or support for more languages!