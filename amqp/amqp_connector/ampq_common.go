package amqp_connector

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"mq_connector/v3/amqp/amqp_config"
	"os"
	"sync/atomic"
	"time"
)

var (
	rmqConnection   *amqp.Connection
	MqConnectionUrl string
	RmqConfig       *amqp_config.RmqServer
)

var (
	errNotConnected  = errors.New("Not connected to the producer")
	errAlreadyClosed = errors.New("Already closed: not connected to the producer")
)
var Rmq *RabbitMessageQueue

type RabbitMessageQueue struct {
	subID            *string
	MqConnectionUrl  string
	MqConfig         *amqp_config.RmqServer
	MqConnection     *amqp.Connection
	MqChannel        *amqp.Channel
	Queue            *amqp.Queue
	Ctx              *context.Context
	ExchangeType     string // fanout, topic, direct, etc...
	Exchange         string
	RoutingKey       string
	QueueName        string
	QueueType        string
	QueueDeclareType string //Direct(simple), WorkQueue, PubSub
	concurrency      int
	ShowLog          bool
	ForceDisconnect  bool // 서비스 강제 연결 끈은 경우
	//Addr          string
	Ttl                int
	IsConnected        bool
	done               chan bool
	notifyClose        chan *amqp.Error
	logger             *log.Logger
	UsePreFix          bool
	Sep                string
	PreFix             string
	ConsumerHandlerFun func(*amqp.Delivery) error
	currentStatus      atomic.Value
	lastRecoverTime    int64

	IsPublisher bool
	IsConsumer  bool
}

func NewRabbitMQ(rmqConfig *amqp_config.RmqServer) *RabbitMessageQueue {
	if rmqConfig.ShowLog {
		log.Printf(">>> NewRabbitMQ <<<")
	}
	rmq := &RabbitMessageQueue{
		logger: log.New(os.Stdout, "", log.LstdFlags),
		done:   make(chan bool),
	}
	rmq.IsPublisher = rmqConfig.IsPublisher
	rmq.IsConsumer = rmqConfig.IsConsumer
	rmq.IsConnected = false
	rmq.RoutingKey = rmqConfig.RoutingKey
	rmq.ExchangeType = rmqConfig.ExchangeType
	if rmq.ExchangeType == "" {
		rmq.ExchangeType = amqp.ExchangeDirect
	}
	rmq.Exchange = rmqConfig.Exchange
	rmq.QueueName = rmqConfig.QueueName
	rmq.QueueType = rmqConfig.QueueType
	rmq.Sep = rmqConfig.RmqPreFix.Sep
	if rmq.Sep == "" {
		rmq.Sep = amqp_config.DefSep
	}
	rmq.PreFix = rmqConfig.RmqPreFix.PreFix
	if rmqConfig.RmqPreFix.UsePreFix == "true" {
		rmq.UsePreFix = true
	}
	rmq.QueueDeclareType = rmqConfig.QueueDeclareType
	if rmq.QueueDeclareType == "" {
		rmq.QueueDeclareType = amqp_config.QueueDeclareTypeWorkQueue
	}
	if rmqConfig.ShowLog {
		rmq.ShowLog = rmqConfig.ShowLog
	}

	// preFix 사용하면
	cErr := rmq.InitSetPreFixed()
	if cErr != nil {
		log.Printf("InitSetPreFixed err %v", cErr)
	}

	rmq.Ttl = amqp_config.DefTtl
	rmq.MqConfig = rmqConfig
	RmqConfig = rmqConfig
	rmq.concurrency = 1

	addr, uErr := GetMqConnectUrlFromConfig(rmqConfig)
	if uErr != nil {
		log.Printf("GetMqConnectUrlFromConfig Err %v", addr)
		rmq.IsConnected = false
		return nil
	}
	if rmq.ShowLog {
		log.Printf("addr : %v", addr)
	}

	if addr == "" {
		log.Printf("MqConnectionUrl nil err")
		rmq.IsConnected = false
		return nil
	}
	MqConnectionUrl = addr
	rmq.MqConnectionUrl = addr
	rmq.QueueType = rmqConfig.QueueType

	conErr := rmq.Connect()
	if conErr != nil {
		log.Printf("rmq.Connect err %v", conErr)
		rmq.IsConnected = false
		return nil
	}
	if rmq.IsConsumer {
		if ok, _ := rmq.waitConn(); ok {
			go rmq.HandleReconnect()
		}
	}

	if rmq.ShowLog {
		log.Printf("Ok new rabbitMQ connector %v", rmq)
	}
	Rmq = rmq
	return rmq
}

func NewConsumer(queue, exchange, routerKey string, consumerHandler *func(*amqp.Delivery) error, newRmqConfig *amqp_config.RmqServer) (err error) {
	if newRmqConfig == nil {
		newRmqConfig = amqp_config.DefaultRabbitMQServerConfigFromEnvWsConnector()
		if newRmqConfig == nil {
			log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", newRmqConfig)
			return errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
		}
		if newRmqConfig.ShowLog {
			log.Printf("rmqConfig:: %v", newRmqConfig)
		}
	}
	if newRmqConfig == nil {
		log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", newRmqConfig)
		return errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
	}
	if newRmqConfig.ShowLog {
		log.Printf(">>> NewConsumer <<< ")
	}
	if queue == "" {
		log.Printf("queue is nil")
		return errors.New("queue is nil")
	}

	consumerMq, setErr := SetNewConsumer(queue, exchange, routerKey, newRmqConfig)
	if setErr != nil {
		log.Printf("SetNewConsumer error %v", setErr)
		return setErr
	}
	if consumerMq == nil {
		log.Printf("SetNewConsumer consumerMq nil %v", consumerMq)
		return errors.New("consumerMq nil")
	}

	if consumerHandler != nil {
		hErr := RunConsumer(consumerMq, consumerHandler)
		if hErr != nil {
			log.Printf("ConsumerHandle error %v", hErr)
		}
	}

	return nil

	//if consumerHandler != nil {
	//	consumerMq.ConsumerHandlerFun = *consumerHandler
	//} else {
	//	consumerMq.ConsumerHandlerFun = DefaultConsumerHandler
	//}
	//
	//rmqMessage, cErr := consumerMq.StartCommonRabbitQueuesConsumer()
	//if cErr != nil {
	//	log.Printf("StartCommonRabbitQueuesConsumer error %v", cErr)
	//}
	//// set Consumer Handler Fun
	//hErr := consumerMq.ConsumerHandle(rmqMessage)
	//if hErr != nil {
	//	log.Printf("ConsumerHandle error %v", hErr)
	//}
	//log.Printf(">>> ConsumerHandle <<<<")
	//return nil

	//if newRmqConfig == nil {
	//	newRmqConfig = amqp_config.DefaultRabbitMQServerConfigFromEnvWsConnector()
	//	if newRmqConfig == nil {
	//		log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", newRmqConfig)
	//		return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
	//	}
	//	if newRmqConfig.ShowLog {
	//		log.Printf("rmqConfig:: %v", newRmqConfig)
	//	}
	//}
	//if newRmqConfig == nil {
	//	log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", newRmqConfig)
	//	return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
	//}
	//rmqConfig := newRmqConfig
	//
	//if queue != "" {
	//	if queue != "" {
	//		rmqConfig.QueueName = queue
	//	}
	//}
	//
	//if exchange != "" {
	//	if exchange != "" {
	//		rmqConfig.Exchange = exchange
	//	}
	//}
	//
	//if routerKey != "" {
	//	if routerKey != "" {
	//		rmqConfig.RoutingKey = routerKey
	//	}
	//}
	//rmqConfig.IsConsumer = true
	//rmqConfig.IsPublisher = false
	//consumerMq := NewRabbitMQ(rmqConfig)
	//fmt.Printf("consumerMq %v", consumerMq)
	//if consumerMq == nil {
	//	fmt.Printf("StartConsumerFromFeApi Err %v", consumerMq)
	//	return
	//}
	////consumerMq.done = make(chan error)
	//consumerMq.lastRecoverTime = time.Now().UTC().Unix()
	//
	//if consumerHandler != nil {
	//	consumerMq.ConsumerHandlerFun = *consumerHandler
	//} else {
	//	consumerMq.ConsumerHandlerFun = DefaultConsumerHandler
	//}
	//
	//rmqMessage, cErr := consumerMq.StartCommonRabbitQueuesConsumer()
	//if cErr != nil {
	//	log.Printf("StartCommonRabbitQueuesConsumer error %v", cErr)
	//}
	//// set Consumer Handler Fun
	//hErr := consumerMq.ConsumerHandle(rmqMessage)
	//if hErr != nil {
	//	log.Printf("ConsumerHandle error %v", hErr)
	//}
	//log.Printf(">>> ConsumerHandle <<<<")
	//return consumerMq, nil
}

// SetNewConsumer : handler 를 추가로 설정해줘야함
func SetNewConsumer(queue, exchange, routerKey string, newRmqConfig *amqp_config.RmqServer) (res *RabbitMessageQueue, err error) {
	if newRmqConfig == nil {
		newRmqConfig = amqp_config.DefaultRabbitMQServerConfigFromEnvWsConnector()
		if newRmqConfig == nil {
			log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", newRmqConfig)
			return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
		}
		if newRmqConfig.ShowLog {
			log.Printf("rmqConfig:: %v", newRmqConfig)
		}
	}
	if newRmqConfig == nil {
		log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", newRmqConfig)
		return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
	}

	if newRmqConfig.ShowLog {
		log.Printf(">>> NewConsumer <<< ")
	}
	if queue == "" {
		log.Printf("queue is nil")
		return res, errors.New("queue is nil")
	}

	if newRmqConfig == nil {
		log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", newRmqConfig)
		return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
	}
	rmqConfig := newRmqConfig

	if queue != "" {
		if queue != "" {
			rmqConfig.QueueName = queue
		}
	}

	if exchange != "" {
		if exchange != "" {
			rmqConfig.Exchange = exchange
		}
	}

	if routerKey != "" {
		if routerKey != "" {
			rmqConfig.RoutingKey = routerKey
		}
	}
	rmqConfig.IsConsumer = true
	rmqConfig.IsPublisher = false
	//fmt.Printf("rmqConfig %v", rmqConfig)

	consumerMq := NewRabbitMQ(rmqConfig)
	if consumerMq == nil {
		fmt.Printf("NewRabbitMQ nil error %v", consumerMq)
		return nil, errors.New("")
	}
	//consumerMq.done = make(chan error)
	consumerMq.lastRecoverTime = time.Now().UTC().Unix()

	cErr := consumerMq.SetCommonRabbitQueuesConsumer()
	if cErr != nil {
		log.Printf("SetCommonRabbitQueuesConsumer error %v", cErr)
		return nil, cErr
	}

	return consumerMq, nil

	//if consumerHandler != nil {
	//	consumerMq.ConsumerHandlerFun = *consumerHandler
	//} else {
	//	consumerMq.ConsumerHandlerFun = DefaultConsumerHandler
	//}
	//
	//rmqMessage, cErr := consumerMq.StartCommonRabbitQueuesConsumer()
	//if cErr != nil {
	//	log.Printf("StartCommonRabbitQueuesConsumer error %v", cErr)
	//	return nil, cErr
	//}
	// set Consumer Handler Fun
	//hErr := consumerMq.ConsumerHandle(rmqMessage)
	//if hErr != nil {
	//	log.Printf("ConsumerHandle error %v", hErr)
	//}
	//log.Printf(">>> ConsumerHandle <<<<")
	//return consumerMq, nil

	//return rmqMessage, nil
}

func RunConsumer(consumerMq *RabbitMessageQueue, consumerHandler *func(*amqp.Delivery) error) (err error) {
	if consumerMq == nil {
		log.Printf("RunConsumer consumerMq nil %v", consumerMq)
		return errors.New("RunConsumer consumerMq err")
	}
	if consumerMq.MqConfig == nil {
		log.Printf("RunConsumer newRmqConfig nil %v", consumerMq.MqConfig)
		return errors.New("RunConsumer newRmqConfig err")
	}
	if consumerMq.MqConfig.ShowLog {
		log.Printf(">>> RunConsumer <<< ")
	}

	//
	//if queue == "" {
	//	log.Printf("queue is nil")
	//	return res, errors.New("queue is nil")
	//}
	//if newRmqConfig == nil {
	//	newRmqConfig = amqp_config.DefaultRabbitMQServerConfigFromEnvWsConnector()
	//	if newRmqConfig == nil {
	//		log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", newRmqConfig)
	//		return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
	//	}
	//	if newRmqConfig.ShowLog {
	//		log.Printf("rmqConfig:: %v", newRmqConfig)
	//	}
	//}
	//if newRmqConfig == nil {
	//	log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", newRmqConfig)
	//	return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
	//}
	//rmqConfig := newRmqConfig
	//
	//if queue != "" {
	//	if queue != "" {
	//		rmqConfig.QueueName = queue
	//	}
	//}
	//
	//if exchange != "" {
	//	if exchange != "" {
	//		rmqConfig.Exchange = exchange
	//	}
	//}
	//
	//if routerKey != "" {
	//	if routerKey != "" {
	//		rmqConfig.RoutingKey = routerKey
	//	}
	//}
	//rmqConfig.IsConsumer = true
	//rmqConfig.IsPublisher = false
	//consumerMq := NewRabbitMQ(rmqConfig)
	//fmt.Printf("consumerMq %v", consumerMq)
	//if consumerMq == nil {
	//	fmt.Printf("StartConsumerFromFeApi Err %v", consumerMq)
	//	return
	//}
	////consumerMq.done = make(chan error)
	//consumerMq.lastRecoverTime = time.Now().UTC().Unix()

	if consumerHandler != nil {
		consumerMq.ConsumerHandlerFun = *consumerHandler
	} else {
		consumerMq.ConsumerHandlerFun = DefaultConsumerHandler
	}

	rmqMessage, cErr := consumerMq.RunCommonRabbitQueuesConsumer()
	if cErr != nil {
		log.Printf("RunCommonRabbitQueuesConsumer error %v", cErr)
		return cErr
	}
	// set Consumer Handler Fun
	hErr := consumerMq.ConsumerHandle(rmqMessage)
	if hErr != nil {
		log.Printf("ConsumerHandle error %v", hErr)
	}
	log.Printf(">>> ConsumerHandle <<<<")
	return nil
}

func NewPublisher(queue, exchange, routerKey, exchangeType string, newRmqConfig *amqp_config.RmqServer) (res *RabbitMessageQueue, err error) {
	if newRmqConfig.ShowLog {
		log.Printf(">>> NewPublisher <<< ")
	}

	if queue == "" {
		log.Printf("queue is nil")
		return res, errors.New("queue is nil")
	}

	if newRmqConfig == nil {
		newRmqConfig = amqp_config.DefaultRabbitMQServerConfigFromEnvWsConnector()
		if newRmqConfig == nil {
			log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", newRmqConfig)
			return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
		}
		if newRmqConfig.ShowLog {
			log.Printf("rmqConfig:: %v", newRmqConfig)
		}
	}
	if newRmqConfig == nil {
		log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", newRmqConfig)
		return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
	}
	rmqConfig := newRmqConfig

	if queue != "" {
		if queue != "" {
			rmqConfig.QueueName = queue
		}
	}

	if exchange != "" {
		if exchange != "" {
			rmqConfig.Exchange = exchange
			if exchangeType != "" {
				if exchangeType != "" {
					rmqConfig.ExchangeType = exchangeType
				}
			}
		}
	}

	if routerKey != "" {
		if routerKey != "" {
			rmqConfig.RoutingKey = routerKey
		}
	}
	rmqConfig.IsConsumer = false
	rmqConfig.IsPublisher = true
	publisherMq := NewRabbitMQ(rmqConfig)
	if publisherMq == nil {
		fmt.Printf("NewRabbitMQ Err %v", publisherMq)
		return
	}
	fmt.Printf("publisherMq %v", publisherMq)

	return publisherMq, nil
}

func DefaultConsumerHandler(msg *amqp.Delivery) error {
	log.Printf(">>> DefaultConsumerHandler <<<")
	log.Printf("msg %v", msg)
	return nil
}

func GetMqConnectUrlFromConfig(rmqConfig *amqp_config.RmqServer) (url string, err error) {
	if rmqConfig == nil {
		log.Printf("RmqConfig is nil Err")
		return url, errors.New("rmqConfig is nil err")
	}
	if rmqConfig.ShowLog {
		log.Printf(">> GetMqConnectUrlFromConfig <<")
	}

	if rmqConfig.Address.Host == "" || rmqConfig.Address.Port == "" {
		log.Printf("RmqConfig.Address.Host is nil Err")
		return url, errors.New("RmqConfig.Address.Host is nil err")
	}
	// first cluster url
	if rmqConfig.MqClusters != "" && len(rmqConfig.MqClusters) > 0 {
		return rmqConfig.MqClusters, nil
	}

	// AWS TLS
	protocal := "amqp"
	if rmqConfig.MqUseTLS {
		protocal = "amqps"
	}
	rtRmqConnString := protocal + "://" + rmqConfig.User + ":" + rmqConfig.Password + "@" + rmqConfig.Address.Host + ":" + rmqConfig.Address.Port + "/"
	if rmqConfig.ShowLog {
		log.Printf("GetMqConfig %v", rtRmqConnString)
	}
	return rtRmqConnString, nil
}
