package main

import (
	"encoding/json"
        "net/http"
	"io/ioutil"
        "sync"
	"strings"
	"time"
	"flag"

	"github.com/Shopify/sarama"
        "github.com/stretchr/graceful"
	"github.com/op/go-logging"
	"github.com/pborman/uuid"
)

var log = logging.MustGetLogger("http2kafka.main")
var logFormat = logging.MustStringFormatter(
	`%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`,
)


type Producer struct {
   AsyncProducer sarama.AsyncProducer
   sync.WaitGroup
}

type Config struct {
	KafkaHosts    []string
	HttpPort  string
	Debug  bool
}

func NewConfigFromEnv() Config {
	kafkaHost := flag.String("kafka-host", "localhost", "Kafka broker to connect to")
	httpPort := flag.String("http-port", ":10025", "Http Port to upstream")
	debug := flag.Bool("debug", false, "Debug HTTP2KAFKA")
	flag.Parse()
	hosts := strings.Split(*kafkaHost, ",")

	return Config{hosts, *httpPort,*debug}
}

func main(){ 
   config := NewConfigFromEnv();
   logging.SetFormatter(logFormat)
   if(config.Debug){	
       logging.SetLevel(logging.INFO, "http2kafka.main")
   }else{
	logging.SetLevel(logging.DEBUG, "http2kafka.main")
   }

   //brokerList := []string{"10.130.18.35:9092"}

   log.Info("Broker list: ",config.KafkaHosts)

   producer := Producer{
      AsyncProducer: GetAsyncProducer(config.KafkaHosts),
   }

   producer.Add(1)

   server := Server{
		TimeoutTime:     5 * time.Second,
                TimeoutStatus:   500,
                TimeoutResponse: "Request timed out.",
		Producer: producer,
		Port:config.HttpPort,
	}

   server.Add(1)
   log.Info("Starting Server %v", server)
   go server.Serve();
   producer.Wait();
   server.Wait();
}

func (p *Producer) ProduceMessageAsync(topic string, val sarama.Encoder){
   message := &sarama.ProducerMessage{
      Topic: topic,
      Value: val,
   }
   p.AsyncProducer.Input() <- message
}

func GetAsyncProducer(brokerList []string) sarama.AsyncProducer {
   config := sarama.NewConfig()
   config.ClientID = uuid.NewRandom().String()
   
   config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
   //config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
   config.Producer.Flush.Frequency = 1000 * time.Millisecond // Flush batches every 500ms
   config.Producer.Flush.MaxMessages = 500000 // Flush batches every 500ms
   config.Producer.Retry.Max = 3
   config.Producer.Flush.Bytes = 350000
   config.Producer.Retry.Backoff = 500 * time.Millisecond
   config.Producer.Partitioner = sarama.NewHashPartitioner
   

   client, err := sarama.NewClient(brokerList, config)
   if err != nil {
                log.Critical(err)
    } else {
                log.Info("Kafka Client connected")
    }

    producer, err := sarama.NewAsyncProducerFromClient(client)
    if err != nil {
                log.Critical(err)
    } else {
                log.Info("Kafka Producer created")
    }

   //defer producer.AsyncClose()

   return producer
}

type Server struct {
        TimeoutTime     time.Duration
        TimeoutStatus   int
        TimeoutResponse string
        Producer        Producer
	Port 		string
        sync.WaitGroup
}


func (s *Server) Handler(w http.ResponseWriter, r *http.Request){
	log.Debug("HTTP Request received")
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
        if err != nil {
            log.Critical("REQUEST BODY ",err)
        }

	/*for k, v := range r.Header {
     	   log.Debugf( "Header field %q, Value %q\n", k, v)
    	}*/
	
	//var m map[string]interface{}
	m := make(map[string]interface{})
        err = json.Unmarshal(b, &m)
	if xforwarderfor:= r.Header.Get("X-Forwarded-For"); xforwarderfor!="" {
		m["x-forwarded-for"] = r.Header.Get("X-Forwarded-For")
        }

	requestJSON, err := json.Marshal(m)
        if err != nil {
           log.Critical("JSON MARSHAL",err)
        }


	log.Debug("Request ready to log: %v", string(requestJSON))

        path := r.URL.Path
        log.Debug("Path:" + path);

        kafkatopic := strings.Replace(path, "/postdata/", "", -1)
        log.Debug("kafkaproducer:" + kafkatopic);

	s.Producer.ProduceMessageAsync(kafkatopic,sarama.StringEncoder(requestJSON))

	//defer  s.Producer.AsyncProducer.Close() // // handle error yourself

        log.Info("Request written in kafka")
	w.WriteHeader(200)
}

func (s *Server) Serve() {
        mux := http.NewServeMux()
        mux.HandleFunc("/", s.Handler)
        graceful.Run(s.Port, s.TimeoutTime, mux)
	s.Done()
}
