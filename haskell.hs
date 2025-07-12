-- Haskell - MQTT & gRPC Streaming Examples
-- Dependencies: mqtt-hs, grpc-haskell, aeson

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}

module StreamingExamples where

import Control.Concurrent (threadDelay, forkIO)
import Control.Concurrent.STM
import Control.Monad (forever, replicateM_)
import Data.Aeson
import Data.Time
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import GHC.Generics
import Network.MQTT.Client
import Network.MQTT.Types
import System.Random

-- ============ MQTT EXAMPLES ============

data SensorData = SensorData
  { timestamp :: Text
  , sensorData :: Double
  , sensor :: Text
  } deriving (Show, Generic)

instance ToJSON SensorData where
  toJSON (SensorData ts d s) = object
    [ "timestamp" .= ts
    , "data" .= d
    , "sensor" .= s
    ]

instance FromJSON SensorData where
  parseJSON = withObject "SensorData" $ \o -> SensorData
    <$> o .: "timestamp"
    <*> o .: "data"
    <*> o .: "sensor"

newtype MQTTPublisher = MQTTPublisher MQTTClient

createMQTTPublisher :: String -> Int -> IO MQTTPublisher
createMQTTPublisher brokerHost brokerPort = do
  let config = mqttConfig
        { _msgCB = SimpleCallback msgReceived
        , _hostname = brokerHost
        , _port = fromIntegral brokerPort
        , _connID = "HaskellPublisher"
        }
  
  client <- connectURI config ("mqtt://" ++ brokerHost ++ ":" ++ show brokerPort)
  putStrLn "MQTT Publisher connected"
  return $ MQTTPublisher client
  where
    msgReceived _ _ _ = return ()

publishSensorData :: MQTTPublisher -> IO ()
publishSensorData (MQTTPublisher client) = forever $ do
  currentTime <- getCurrentTime
  randomValue <- randomRIO (0.0, 100.0)
  
  let sensorData = SensorData
        { timestamp = T.pack $ formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" currentTime
        , sensorData = randomValue
        , sensor = "temperature"
        }
  
  let jsonData = encode sensorData
  publishq client "sensors/temperature" jsonData False QoS1 []
  
  putStrLn $ "Published: " ++ show jsonData
  threadDelay 2000000 -- 2 seconds

newtype MQTTSubscriber = MQTTSubscriber MQTTClient

createMQTTSubscriber :: String -> Int -> IO MQTTSubscriber
createMQTTSubscriber brokerHost brokerPort = do
  let config = mqttConfig
        { _msgCB = SimpleCallback msgReceived
        , _hostname = brokerHost
        , _port = fromIntegral brokerPort
        , _connID = "HaskellSubscriber"
        }
  
  client <- connectURI config ("mqtt://" ++ brokerHost ++ ":" ++ show brokerPort)
  subscribe client [("sensors/+", subOptions)] []
  putStrLn "MQTT Subscriber connected and listening..."
  return $ MQTTSubscriber client
  where
    msgReceived client topic payload _props = do
      case decode payload of
        Just sensorData -> do
          putStrLn $ "Received on " ++ T.unpack topic ++ ": " ++
                    "timestamp=" ++ T.unpack (timestamp sensorData) ++
                    ", data=" ++ show (sensorData sensorData) ++
                    ", sensor=" ++ T.unpack (sensor sensorData)
        Nothing -> putStrLn "Error parsing message"

-- ============ gRPC EXAMPLES ============

-- Note: In a real implementation, you would generate these from .proto files
-- using the protobuf compiler

data StreamRequest = StreamRequest
  { requestMessage :: Text
  , requestTimestamp :: Text
  } deriving (Show, Generic)

instance ToJSON StreamRequest
instance FromJSON StreamRequest

data StreamResponse = StreamResponse
  { responseMessage :: Text
  , responseTimestamp :: Text
  , responseCount :: Int
  } deriving (Show, Generic)

instance ToJSON StreamResponse
instance FromJSON StreamResponse

-- Mock gRPC service implementation
data StreamingService = StreamingService

serverStream :: StreamingService -> StreamRequest -> IO [StreamResponse]
serverStream _ request = do
  putStrLn $ "Server streaming request: " ++ T.unpack (requestMessage request)
  
  responses <- mapM createResponse [0..9]
  return responses
  where
    createResponse i = do
      currentTime <- getCurrentTime
      threadDelay 1000000 -- 1 second
      return $ StreamResponse
        { responseMessage = "Stream message " <> T.pack (show i)
        , responseTimestamp = T.pack $ formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" currentTime
        , responseCount = i
        }

bidirectionalStream :: StreamingService -> [StreamRequest] -> IO [StreamResponse]
bidirectionalStream _ requests = do
  mapM processRequest requests
  where
    processRequest request = do
      putStrLn $ "Received: " ++ T.unpack (requestMessage request)
      currentTime <- getCurrentTime
      return $ StreamResponse
        { responseMessage = "Echo: " <> requestMessage request
        , responseTimestamp = T.pack $ formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" currentTime
        , responseCount = 0
        }

-- Mock gRPC server
data GRPCServer = GRPCServer StreamingService

startGRPCServer :: IO GRPCServer
startGRPCServer = do
  -- Note: This is a simplified example
  -- In a real implementation, you would use the generated gRPC server code
  
  putStrLn "gRPC Server running on port 50051"
  let service = StreamingService
  return $ GRPCServer service

-- Mock gRPC client
data GRPCClient = GRPCClient

connectGRPCClient :: IO GRPCClient
connectGRPCClient = do
  -- Note: This is a simplified example
  -- In a real implementation, you would use the generated gRPC client code
  
  putStrLn "gRPC Client connecting to localhost:50051"
  return GRPCClient

grpcServerStream :: GRPCClient -> IO ()
grpcServerStream _ = do
  putStrLn "Starting server stream..."
  
  currentTime <- getCurrentTime
  let request = StreamRequest
        { requestMessage = "Start streaming"
        , requestTimestamp = T.pack $ formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" currentTime
        }
  
  -- Mock server streaming call
  replicateM_ 10 $ do
    putStrLn "Server stream response: Stream message"
    threadDelay 1000000
  
  -- Real implementation would be:
  -- responses <- serverStream service request
  -- mapM_ (\resp -> putStrLn $ "Server stream response: " ++ T.unpack (responseMessage resp)) responses

grpcBidirectionalStream :: GRPCClient -> IO ()
grpcBidirectionalStream _ = do
  putStrLn "Starting bidirectional stream..."
  
  -- Mock bidirectional streaming
  requests <- mapM createRequest [0..4]
  
  mapM_ processRequest requests
  where
    createRequest i = do
      currentTime <- getCurrentTime
      threadDelay 2000000 -- 2 seconds
      return $ StreamRequest
        { requestMessage = "Client message " <> T.pack (show i)
        , requestTimestamp = T.pack $ formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%QZ" currentTime
        }
    
    processRequest request = do
      putStrLn $ "Sending: " ++ T.unpack (requestMessage request)
      putStrLn $ "Bidirectional response: Echo: " ++ T.unpack (requestMessage request)
  
  -- Real implementation would be:
  -- responses <- bidirectionalStream service requests
  -- mapM_ (\resp -> putStrLn $ "Bidirectional response: " ++ T.unpack (responseMessage resp)) responses

-- Example usage
main :: IO ()
main = do
  putStrLn "Haskell MQTT & gRPC streaming examples ready"
  
  -- MQTT Examples
  -- publisher <- createMQTTPublisher "localhost" 1883
  -- _ <- forkIO $ publishSensorData publisher
  
  -- subscriber <- createMQTTSubscriber "localhost" 1883
  
  -- gRPC Examples
  -- server <- startGRPCServer
  -- threadDelay 1000000 -- Wait for server to start
  
  -- client <- connectGRPCClient
  -- grpcServerStream client
  -- grpcBidirectionalStream client
  
  -- Keep the main thread alive
  -- threadDelay maxBound
  return ()