%% Erlang - MQTT & gRPC Streaming Examples
%% Dependencies: {emqtt, "1.4.4"}, {grpcbox, "0.15.0"}

-module(streaming_examples).
-export([start/0, mqtt_publisher/1, mqtt_subscriber/1, grpc_server/0, grpc_client/0]).

-record(sensor_data, {
    timestamp :: binary(),
    data :: float(),
    sensor :: binary()
}).

-record(stream_request, {
    message :: binary(),
    timestamp :: binary()
}).

-record(stream_response, {
    message :: binary(),
    timestamp :: binary(),
    count :: integer()
}).

%% ============ MQTT EXAMPLES ============

create_sensor_data(Data, Sensor) ->
    Timestamp = list_to_binary(calendar:system_time_to_rfc3339(erlang:system_time(second))),
    #sensor_data{
        timestamp = Timestamp,
        data = Data,
        sensor = Sensor
    }.

sensor_data_to_json(#sensor_data{timestamp = Timestamp, data = Data, sensor = Sensor}) ->
    jsx:encode(#{
        <<"timestamp">> => Timestamp,
        <<"data">> => Data,
        <<"sensor">> => Sensor
    }).

json_to_sensor_data(Json) ->
    try
        Map = jsx:decode(Json),
        #sensor_data{
            timestamp = maps:get(<<"timestamp">>, Map),
            data = maps:get(<<"data">>, Map),
            sensor = maps:get(<<"sensor">>, Map)
        }
    catch
        _:_ ->
            io:format("Error parsing JSON~n"),
            undefined
    end.

mqtt_publisher(BrokerHost) ->
    {ok, Client} = emqtt:start_link([
        {host, BrokerHost},
        {port, 1883},
        {clientid, <<"ErlangPublisher">>}
    ]),
    
    {ok, _} = emqtt:connect(Client),
    io:format("MQTT Publisher connected~n"),
    
    publish_loop(Client).

publish_loop(Client) ->
    Data = create_sensor_data(rand:uniform() * 100, <<"temperature">>),
    Json = sensor_data_to_json(Data),
    
    case emqtt:publish(Client, <<"sensors/temperature">>, Json, 1) of
        ok ->
            io:format("Published: ~s~n", [Json]);
        {error, Reason} ->
            io:format("Error publishing: ~p~n", [Reason])
    end,
    
    timer:sleep(2000),
    publish_loop(Client).

mqtt_subscriber(BrokerHost) ->
    {ok, Client} = emqtt:start_link([
        {host, BrokerHost},
        {port, 1883},
        {clientid, <<"ErlangSubscriber">>}
    ]),
    
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"sensors/+">>, 1),
    io:format("MQTT Subscriber connected and listening...~n"),
    
    subscribe_loop(Client).

subscribe_loop(Client) ->
    receive
        {publish, #{topic := Topic, payload := Payload}} ->
            case json_to_sensor_data(Payload) of
                #sensor_data{timestamp = Timestamp, data = Data, sensor = Sensor} ->
                    io:format("Received on ~s: timestamp=~s, data=~p, sensor=~s~n", 
                             [Topic, Timestamp, Data, Sensor]);
                undefined ->
                    ok
            end,
            subscribe_loop(Client);
        _ ->
            subscribe_loop(Client)
    end.

%% ============ gRPC EXAMPLES ============

%% Note: In a real implementation, you would generate these from .proto files
%% using the protobuf compiler

create_stream_request(Message) ->
    Timestamp = list_to_binary(calendar:system_time_to_rfc3339(erlang:system_time(second))),
    #stream_request{
        message = Message,
        timestamp = Timestamp
    }.

create_stream_response(Message, Count) ->
    Timestamp = list_to_binary(calendar:system_time_to_rfc3339(erlang:system_time(second))),
    #stream_response{
        message = Message,
        timestamp = Timestamp,
        count = Count
    }.

%% Mock gRPC service implementation
server_stream(#stream_request{message = Message}) ->
    io:format("Server streaming request: ~s~n", [Message]),
    
    [begin
         timer:sleep(1000),
         create_stream_response(
             list_to_binary(io_lib:format("Stream message ~p", [I])), 
             I
         )
     end || I <- lists:seq(0, 9)].

bidirectional_stream(Requests) ->
    [begin
         io:format("Received: ~s~n", [Request#stream_request.message]),
         create_stream_response(
             iolist_to_binary([<<"Echo: ">>, Request#stream_request.message]),
             0
         )
     end || Request <- Requests].

grpc_server() ->
    %% Note: This is a simplified example
    %% In a real implementation, you would use the generated gRPC server code
    
    io:format("gRPC Server running on port 50051~n"),
    
    %% Mock server loop
    server_loop().

server_loop() ->
    timer:sleep(1000),
    server_loop().

grpc_client() ->
    %% Note: This is a simplified example
    %% In a real implementation, you would use the generated gRPC client code
    
    io:format("gRPC Client connecting to localhost:50051~n"),
    
    grpc_server_stream(),
    grpc_bidirectional_stream().

grpc_server_stream() ->
    io:format("Starting server stream...~n"),
    
    Request = create_stream_request(<<"Start streaming">>),
    
    %% Mock server streaming call
    [begin
         io:format("Server stream response: Stream message ~p~n", [I]),
         timer:sleep(1000)
     end || I <- lists:seq(0, 9)].
    
    %% Real implementation would be:
    %% Responses = server_stream(Request),
    %% [io:format("Server stream response: ~s~n", [Response#stream_response.message]) 
    %%  || Response <- Responses].

grpc_bidirectional_stream() ->
    io:format("Starting bidirectional stream...~n"),
    
    %% Mock bidirectional streaming
    Requests = [begin
                    timer:sleep(2000),
                    create_stream_request(
                        list_to_binary(io_lib:format("Client message ~p", [I]))
                    )
                end || I <- lists:seq(0, 4)],
    
    [begin
         io:format("Sending: ~s~n", [Request#stream_request.message]),
         io:format("Bidirectional response: Echo: ~s~n", [Request#stream_request.message])
     end || Request <- Requests].
    
    %% Real implementation would be:
    %% Responses = bidirectional_stream(Requests),
    %% [io:format("Bidirectional response: ~s~n", [Response#stream_response.message]) 
    %%  || Response <- Responses].

%% Example usage
start() ->
    io:format("Erlang MQTT & gRPC streaming examples ready~n"),
    
    %% MQTT Examples
    %% spawn(fun() -> mqtt_publisher("localhost") end),
    %% spawn(fun() -> mqtt_subscriber("localhost") end),
    
    %% gRPC Examples
    %% spawn(fun() -> grpc_server() end),
    %% timer:sleep(1000), %% Wait for server to start
    %% grpc_client(),
    
    ok.