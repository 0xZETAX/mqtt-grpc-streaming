;; Clojure - MQTT & gRPC Streaming Examples
;; Dependencies: [clojurewerkz/machine_head "1.0.0"] [io.grpc/grpc-all "1.45.0"]

(ns streaming-examples
  (:require [clojure.data.json :as json]
            [clojure.core.async :as async :refer [go go-loop <! >! timeout chan]]
            [clojurewerkz.machine-head.client :as mh])
  (:import [java.time Instant]
           [java.util.concurrent Executors TimeUnit]
           [io.grpc Server ServerBuilder ManagedChannelBuilder]
           [io.grpc.stub StreamObserver]))

;; ============ MQTT EXAMPLES ============

(defn create-sensor-data [data sensor]
  {:timestamp (.toString (Instant/now))
   :data data
   :sensor sensor})

(defn sensor-data-to-json [sensor-data]
  (json/write-str sensor-data))

(defn json-to-sensor-data [json-str]
  (try
    (json/read-str json-str :key-fn keyword)
    (catch Exception e
      (println "Error parsing JSON:" (.getMessage e))
      nil)))

(defn create-mqtt-publisher [broker-url]
  (let [client (mh/connect broker-url "ClojurePublisher")]
    (println "MQTT Publisher connected")
    client))

(defn publish-sensor-data [client]
  (let [executor (Executors/newScheduledThreadPool 1)]
    (.scheduleAtFixedRate executor
      (fn []
        (let [data (create-sensor-data (* (rand) 100) "temperature")
              json (sensor-data-to-json data)]
          (try
            (mh/publish client "sensors/temperature" json 1 false)
            (println "Published:" json)
            (catch Exception e
              (println "Error publishing:" (.getMessage e))))))
      0 2 TimeUnit/SECONDS)))

(defn create-mqtt-subscriber [broker-url]
  (let [client (mh/connect broker-url "ClojureSubscriber")]
    (mh/subscribe client
      {"sensors/+" (fn [^String topic ^bytes payload]
                     (let [message (String. payload)
                           data (json-to-sensor-data message)]
                       (when data
                         (println (format "Received on %s: timestamp=%s, data=%s, sensor=%s"
                                        topic (:timestamp data) (:data data) (:sensor data)))))})
    (println "MQTT Subscriber connected and listening...")
    client))

;; ============ gRPC EXAMPLES ============

;; Note: In a real implementation, you would generate these from .proto files
;; using the protobuf compiler

(defrecord StreamRequest [message timestamp])
(defrecord StreamResponse [message timestamp count])

(defn create-stream-request [message]
  (->StreamRequest message (.toString (Instant/now))))

(defn create-stream-response [message count]
  (->StreamResponse message (.toString (Instant/now)) count))

;; Mock gRPC service implementation
(defprotocol StreamingService
  (server-stream [this request])
  (bidirectional-stream [this requests]))

(deftype StreamingServiceImpl []
  StreamingService
  (server-stream [this request]
    (println "Server streaming request:" (:message request))
    (for [i (range 10)]
      (do
        (Thread/sleep 1000)
        (create-stream-response (str "Stream message " i) i))))
  
  (bidirectional-stream [this requests]
    (map (fn [request]
           (println "Received:" (:message request))
           (create-stream-response (str "Echo: " (:message request)) 0))
         requests)))

(defn start-grpc-server []
  ;; Note: This is a simplified example
  ;; In a real implementation, you would use the generated gRPC server code
  
  (println "gRPC Server running on port 50051")
  (let [service (StreamingServiceImpl.)
        executor (Executors/newSingleThreadExecutor)]
    ;; Mock server loop
    (.submit executor
      (fn []
        (loop []
          (Thread/sleep 1000)
          (recur))))))

(defn connect-grpc-client []
  ;; Note: This is a simplified example
  ;; In a real implementation, you would use the generated gRPC client code
  
  (println "gRPC Client connecting to localhost:50051")
  ;; Return mock client
  {})

(defn grpc-server-stream [client]
  (println "Starting server stream...")
  
  (let [request (create-stream-request "Start streaming")]
    ;; Mock server streaming call
    (doseq [i (range 10)]
      (println (str "Server stream response: Stream message " i))
      (Thread/sleep 1000))
    
    ;; Real implementation would be:
    ;; (let [responses (server-stream service request)]
    ;;   (doseq [response responses]
    ;;     (println "Server stream response:" (:message response))))))

(defn grpc-bidirectional-stream [client]
  (println "Starting bidirectional stream...")
  
  ;; Mock bidirectional streaming
  (let [requests (for [i (range 5)]
                   (do
                     (Thread/sleep 2000)
                     (create-stream-request (str "Client message " i))))]
    
    (doseq [request requests]
      (println "Sending:" (:message request))
      (println "Bidirectional response: Echo:" (:message request)))
    
    ;; Real implementation would be:
    ;; (let [responses (bidirectional-stream service requests)]
    ;;   (doseq [response responses]
    ;;     (println "Bidirectional response:" (:message response))))))

;; Example usage
(defn -main []
  (println "Clojure MQTT & gRPC streaming examples ready")
  
  ;; MQTT Examples
  ;; (let [publisher (create-mqtt-publisher "tcp://localhost:1883")]
  ;;   (publish-sensor-data publisher))
  
  ;; (let [subscriber (create-mqtt-subscriber "tcp://localhost:1883")])
  
  ;; gRPC Examples
  ;; (start-grpc-server)
  ;; (Thread/sleep 1000) ; Wait for server to start
  
  ;; (let [client (connect-grpc-client)]
  ;;   (grpc-server-stream client)
  ;;   (grpc-bidirectional-stream client))
  
  ;; Keep the main thread alive
  ;; (Thread/sleep Long/MAX_VALUE)
  )

;; Run if this file is executed directly
(when (= *file* (System/getProperty "babashka.file"))
  (-main))