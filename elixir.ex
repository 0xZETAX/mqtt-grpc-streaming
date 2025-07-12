# Elixir - MQTT & gRPC Streaming Examples
# Dependencies: {:tortoise, "~> 0.9"}, {:grpc, "~> 0.5"}

defmodule StreamingExamples do
  @moduledoc """
  MQTT and gRPC streaming examples in Elixir
  """

  # ============ MQTT EXAMPLES ============

  defmodule SensorData do
    @derive Jason.Encoder
    defstruct [:timestamp, :data, :sensor]

    def new(data, sensor \\ "temperature") do
      %__MODULE__{
        timestamp: DateTime.utc_now() |> DateTime.to_iso8601(),
        data: data,
        sensor: sensor
      }
    end

    def from_json(json_string) do
      case Jason.decode(json_string) do
        {:ok, %{"timestamp" => timestamp, "data" => data, "sensor" => sensor}} ->
          {:ok, %__MODULE__{timestamp: timestamp, data: data, sensor: sensor}}
        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defmodule MQTTPublisher do
    use GenServer
    require Logger

    def start_link(broker_host \\ "localhost", broker_port \\ 1883) do
      GenServer.start_link(__MODULE__, {broker_host, broker_port}, name: __MODULE__)
    end

    def publish_sensor_data do
      GenServer.cast(__MODULE__, :publish_sensor_data)
    end

    def stop do
      GenServer.stop(__MODULE__)
    end

    @impl true
    def init({broker_host, broker_port}) do
      client_id = "ElixirPublisher"
      
      {:ok, _pid} = Tortoise.Supervisor.start_child(
        client_id: client_id,
        handler: {Tortoise.Handler.Logger, []},
        server: {Tortoise.Transport.Tcp, host: broker_host, port: broker_port},
        subscriptions: []
      )

      Logger.info("MQTT Publisher connected")
      {:ok, %{client_id: client_id}}
    end

    @impl true
    def handle_cast(:publish_sensor_data, %{client_id: client_id} = state) do
      # Schedule periodic publishing
      Process.send_after(self(), :publish, 0)
      {:noreply, state}
    end

    @impl true
    def handle_info(:publish, %{client_id: client_id} = state) do
      data = SensorData.new(:rand.uniform() * 100)
      json = Jason.encode!(data)

      case Tortoise.publish(client_id, "sensors/temperature", json, qos: 1) do
        :ok ->
          Logger.info("Published: #{json}")
        {:error, reason} ->
          Logger.error("Error publishing: #{inspect(reason)}")
      end

      # Schedule next publish
      Process.send_after(self(), :publish, 2000)
      {:noreply, state}
    end
  end

  defmodule MQTTSubscriber do
    use GenServer
    require Logger

    def start_link(broker_host \\ "localhost", broker_port \\ 1883) do
      GenServer.start_link(__MODULE__, {broker_host, broker_port}, name: __MODULE__)
    end

    def stop do
      GenServer.stop(__MODULE__)
    end

    @impl true
    def init({broker_host, broker_port}) do
      client_id = "ElixirSubscriber"
      
      {:ok, _pid} = Tortoise.Supervisor.start_child(
        client_id: client_id,
        handler: {__MODULE__.Handler, []},
        server: {Tortoise.Transport.Tcp, host: broker_host, port: broker_port},
        subscriptions: [{"sensors/+", 1}]
      )

      Logger.info("MQTT Subscriber connected and listening...")
      {:ok, %{client_id: client_id}}
    end

    defmodule Handler do
      use Tortoise.Handler
      require Logger

      @impl true
      def handle_message(topic, payload, state) do
        case SensorData.from_json(payload) do
          {:ok, data} ->
            Logger.info("Received on #{topic}: timestamp=#{data.timestamp}, data=#{data.data}, sensor=#{data.sensor}")
          {:error, reason} ->
            Logger.error("Error parsing message: #{inspect(reason)}")
        end
        {:ok, state}
      end
    end
  end

  # ============ gRPC EXAMPLES ============

  # Note: In a real implementation, you would generate these from .proto files
  # using: protoc --elixir_out=plugins=grpc:./lib --proto_path=./priv/protos ./priv/protos/streaming.proto

  defmodule StreamRequest do
    defstruct [:message, :timestamp]

    def new(message) do
      %__MODULE__{
        message: message,
        timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
      }
    end
  end

  defmodule StreamResponse do
    defstruct [:message, :timestamp, :count]

    def new(message, count \\ 0) do
      %__MODULE__{
        message: message,
        timestamp: DateTime.utc_now() |> DateTime.to_iso8601(),
        count: count
      }
    end
  end

  # Mock gRPC service implementation
  defmodule StreamingService do
    require Logger

    def server_stream(request) do
      Logger.info("Server streaming request: #{request.message}")
      
      Stream.iterate(0, &(&1 + 1))
      |> Stream.take(10)
      |> Stream.map(fn i ->
        Process.sleep(1000)
        StreamResponse.new("Stream message #{i}", i)
      end)
    end

    def bidirectional_stream(request_stream) do
      request_stream
      |> Stream.map(fn request ->
        Logger.info("Received: #{request.message}")
        StreamResponse.new("Echo: #{request.message}")
      end)
    end
  end

  defmodule GRPCServer do
    use GenServer
    require Logger

    def start_link do
      GenServer.start_link(__MODULE__, [], name: __MODULE__)
    end

    def stop do
      GenServer.stop(__MODULE__)
    end

    @impl true
    def init([]) do
      # Note: This is a simplified example
      # In a real implementation, you would use the generated gRPC server code
      
      Logger.info("gRPC Server running on port 50051")
      
      # Mock server process
      {:ok, %{}}
    end
  end

  defmodule GRPCClient do
    require Logger

    def connect do
      # Note: This is a simplified example
      # In a real implementation, you would use the generated gRPC client code
      
      Logger.info("gRPC Client connecting to localhost:50051")
      :ok
    end

    def server_stream do
      Logger.info("Starting server stream...")
      
      request = StreamRequest.new("Start streaming")
      
      # Mock server streaming call
      0..9
      |> Enum.each(fn i ->
        Logger.info("Server stream response: Stream message #{i}")
        Process.sleep(1000)
      end)
      
      # Real implementation would be:
      # {:ok, channel} = GRPC.Stub.connect("localhost:50051")
      # request = StreamRequest.new("Start streaming")
      # {:ok, stream} = StreamingService.Stub.server_stream(channel, request)
      # 
      # stream
      # |> Enum.each(fn {:ok, response} ->
      #   Logger.info("Server stream response: #{response.message}")
      # end)
    end

    def bidirectional_stream do
      Logger.info("Starting bidirectional stream...")
      
      # Mock bidirectional streaming
      requests = Stream.iterate(0, &(&1 + 1))
      |> Stream.take(5)
      |> Stream.map(fn i ->
        Process.sleep(2000)
        StreamRequest.new("Client message #{i}")
      end)
      
      requests
      |> Enum.each(fn request ->
        Logger.info("Sending: #{request.message}")
        Logger.info("Bidirectional response: Echo: #{request.message}")
      end)
      
      # Real implementation would be:
      # {:ok, channel} = GRPC.Stub.connect("localhost:50051")
      # {:ok, stream} = StreamingService.Stub.bidirectional_stream(channel)
      # 
      # # Send requests
      # requests
      # |> Enum.each(fn request ->
      #   GRPC.Stub.send_request(stream, request)
      # end)
      # 
      # # Receive responses
      # stream
      # |> Enum.each(fn {:ok, response} ->
      #   Logger.info("Bidirectional response: #{response.message}")
      # end)
    end
  end

  # Example usage
  def main do
    IO.puts("Elixir MQTT & gRPC streaming examples ready")
    
    # MQTT Examples
    # {:ok, _} = MQTTPublisher.start_link()
    # MQTTPublisher.publish_sensor_data()
    
    # {:ok, _} = MQTTSubscriber.start_link()
    
    # gRPC Examples
    # {:ok, _} = GRPCServer.start_link()
    # Process.sleep(1000) # Wait for server to start
    
    # GRPCClient.connect()
    # GRPCClient.server_stream()
    # GRPCClient.bidirectional_stream()
    
    # Keep the application running
    # Process.sleep(:infinity)
  end
end

# Run if this file is executed directly
if __ENV__.file == Path.absname(__ENV__.file) do
  StreamingExamples.main()
end