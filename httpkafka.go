package main

import (
	"encoding/json"
        "net/http"
	"io/ioutil"
        "sync"
	"strings"
	"time"

	"github.com/Shopify/sarama"
        "github.com/stretchr/graceful"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("http2kafka.main")
var logFormat = logging.MustStringFormatter(
	`%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}`,
)


type Producer struct {
   AsyncProducer sarama.AsyncProducer
   sync.WaitGroup
}

func main(){

   logging.SetFormatter(logFormat)
   logging.SetLevel(logging.ERROR, "http2kafka.main")

   brokerList := []string{"10.130.18.33:9092"}

   log.Debug("Broker list: ",brokerList)

   producer := Producer{
      AsyncProducer: GetAsyncProducer(brokerList),
   }

   producer.Add(1)

   server := Server{
		TimeoutTime:     5 * time.Second,
                TimeoutStatus:   500,
                TimeoutResponse: "Request timed out.",
		Producer: producer,
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
   config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
   config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
   config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

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

   return producer
}

type Server struct {
        TimeoutTime     time.Duration
        TimeoutStatus   int
        TimeoutResponse string
        Producer        Producer
        sync.WaitGroup
}


func (s *Server) Handler(w http.ResponseWriter, r *http.Request){
	log.Debug("HTTP Request received")
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
        if err != nil {
            log.Critical("REQUEST BODY ",err)
        }

	for k, v := range r.Header {
     	   log.Debugf( "Header field %q, Value %q\n", k, v)
    	}
	
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

        log.Debug("Request written in kafka")
	//w.WriteHeader(200)
}

func (s *Server) Serve() {
        mux := http.NewServeMux()
        mux.HandleFunc("/", s.Handler)
        graceful.Run(":10025", s.TimeoutTime, mux)
	s.Done()
}
