package amqp_connector

import (
	"context"
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math/rand"
	"mq_connector/v3/amqp/amqp_config"
	"time"
)

func (rq *RabbitMessageQueue) Connect() error {
	if rq.ShowLog {
		log.Printf(">>> Connect <<<")
	}
	//mqConnection := NewConnection(true)
	if rq.MqConnectionUrl == "" {
		log.Printf("rq.MqConnectionUrl is nil %v", rq.MqConnectionUrl)
		return errors.New("rq.MqConnectionUrl is nil")
	}
	addr := rq.MqConnectionUrl
	rq.IsConnected = false
	if rq.ShowLog {
		log.Printf("addr %v", addr)
	}
	//AWS TLS
	var mqConn *amqp.Connection
	var errConn error = nil
	if rq.MqConfig.MqUseTLS {
		tlsCfg, loadErr := TLSLoadConfig(rq.MqConfig.MqTLSRootCAsFile, rq.MqConfig.MqTLSCertFile, rq.MqConfig.MqTLSKeyFile)
		if loadErr != nil {
			log.Printf("tlsCfg load Err: %v", loadErr.Error())
			rq.IsConnected = false
			return loadErr
		}
		if tlsCfg != nil {
			if rq.ShowLog {
				log.Printf("tlsCfg %v", tlsCfg)
				log.Printf("addr %v", addr)
			}
			mqConn, errConn = amqp.DialTLS(addr, tlsCfg)
		} else {
			return errors.New("tls config is nil")
		}
	} else {
		mqConn, errConn = amqp.Dial(addr)
	}
	if errConn != nil {
		rq.IsConnected = false
		log.Printf("error amqp.Dial %v", errConn)
		return errors.New("amqp.Dial conn err")
	}
	if mqConn == nil {
		rq.IsConnected = false
		log.Printf("error mqConn %v", errConn)
		return errors.New("amqp conn err")
	}
	rmqConnection = mqConn

	if rq.ShowLog {
		log.Printf("rmqConnection %v", rmqConnection)
	}

	mqChannel, errChannel := rmqConnection.Channel()
	if errChannel != nil {
		rq.IsConnected = false
		log.Printf("back_mq channel nil err %v", errChannel)
		return errors.New("back_mq channel nil err")
	}

	//rq.MqConnection = rmqConnection
	//rq.MqChannel = mqChannel
	//rq.notifyClose = make(chan *amqp.Error)
	//rq.MqChannel.NotifyClose(rq.notifyClose)
	rq.changeConnection(rmqConnection, mqChannel)

	rq.IsConnected = true
	rq.concurrency = 1
	ctx := context.Background()
	rq.Ctx = &ctx
	if rq.ShowLog {
		log.Printf("mq Connect success")
	}

	return nil
}

func (rq *RabbitMessageQueue) ReConnect(queueName, routingKey string, retryTime int) error {
	if rq.ShowLog {
		log.Printf(">>> ReConnect <<<")
	}
	if rq.ForceDisconnect == true {
		return nil
	}
	rq.MqChannel.Close()
	rq.MqConnection.Close()
	rq.IsConnected = false

	time.Sleep(time.Duration(15+rand.Intn(60)+2*retryTime) * time.Second)
	if rq.ShowLog {
		log.Printf("Try ReConnect with times: %v", retryTime)
	}
	if queueName != "" {
		rq.QueueName = queueName
	}
	if routingKey != "" {
		rq.RoutingKey = routingKey
	}

	cErr := rq.Connect()
	if cErr != nil {
		log.Printf("ReConnect Connect Error %v", cErr)
		return cErr
	}
	if rq.IsConnected != true || rq.MqConnection == nil || rq.MqChannel == nil {
		cErr = rq.Connect()
		if cErr != nil {
			log.Printf("ReConnect Connect Error %v", cErr)
			return cErr
		}
	}
	if rq.IsConnected != true || rq.MqConnection == nil || rq.MqChannel == nil {
		log.Printf("NotConnected Connect Error %v", rq.MqConnection)
		return errors.New("NotConnected Connect Error")
	}

	// consumer 인 경우 핸들러를 재등록해준다.
	if rq.IsConsumer {
		rq.lastRecoverTime = time.Now().UTC().Unix()
		if rq.ConsumerHandlerFun == nil {
			rq.ConsumerHandlerFun = DefaultConsumerHandler
		}

		rmqMessage, ccErr := rq.StartCommonRabbitQueuesConsumer()
		if ccErr != nil {
			log.Printf("StartCommonRabbitQueuesConsumer error %v", ccErr)
			return ccErr
		}
		// set Consumer Handler Fun
		hErr := rq.ConsumerHandle(rmqMessage)
		if hErr != nil {
			log.Printf("ConsumerHandle error %v", hErr)
		}
		log.Printf(">>> ReConnect ConsumerHandle <<<<")
	}

	return nil
}

func (rq *RabbitMessageQueue) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {

	rq.MqConnection = connection
	rq.MqChannel = channel

	rq.notifyClose = make(chan *amqp.Error)
	rq.MqChannel.NotifyClose(rq.notifyClose)
}

func (rq *RabbitMessageQueue) HandleReconnect() {
	if rq.ShowLog {
		log.Printf(">>> HandleReconnect <<<")
	}
	for {
		if rq.ForceDisconnect == true {
			//rq.done <- true
		} else {
			if rq.IsConnected == false || rq.MqConnection.IsClosed() {
				log.Println("Amqp to connect")
				retryTime := 1
				cErr := rq.ReConnect(rq.QueueName, rq.RoutingKey, retryTime)
				if cErr != nil {
					log.Println("Failed to connect. Retrying... %v", cErr)
					retryTime += 1
				}
			}
			time.Sleep(amqp_config.ReconnectDelay)
		}

		select {
		case <-rq.done:
			return
		case <-rq.notifyClose:
			rq.MqChannel.Close()
			rq.MqConnection.Close()
			rq.IsConnected = false
		}
	}
}

func (rq *RabbitMessageQueue) Close() error {
	if !rq.IsConnected {
		return errAlreadyClosed
	}
	rq.ForceDisconnect = true
	err := rq.MqChannel.Close()
	if err != nil {
		return err
	}
	err = rq.MqConnection.Close()
	if err != nil {
		return err
	}
	close(rq.done)
	rq.IsConnected = false
	return nil
}

func (rq *RabbitMessageQueue) DeleteQueue(queueName string) (int, error) {
	if ok, err := rq.waitConn(); !ok {
		return 0, err
	}
	_queue_name := rq.QueueName
	if queueName != "" {
		_queue_name = queueName
	}

	return rq.MqChannel.QueueDelete(_queue_name, false, false, false)
}

func (rq *RabbitMessageQueue) DeleteExchange() error {
	if ok, err := rq.waitConn(); !ok {
		return err
	}
	_exchange_name := rq.Exchange
	if _exchange_name == "" {
		return errors.New("empty _exchange_name")
	}

	return rq.MqChannel.ExchangeDelete(_exchange_name, true, false)
}

func (rq *RabbitMessageQueue) QueueUnbind() error {
	if ok, err := rq.waitConn(); !ok {
		return err
	}
	_name := rq.QueueName
	_route := rq.RoutingKey
	_exchange := rq.Exchange

	return rq.MqChannel.QueueUnbind(_name, _route, _exchange, nil)
}

func (rq *RabbitMessageQueue) waitConn() (bool, error) {

	var _flag = false
	var i = 1
Loop:
	for {
		if i == amqp_config.MaxWaitNum {
			break Loop
		}
		if _flag != rq.IsConnected {
			_flag = true
			break Loop
		}
		i++
		time.Sleep(1 * time.Second)
	}
	if !rq.IsConnected {
		return _flag, errNotConnected
	}
	return _flag, nil
}

// ConnClose : ConnClose
func (rq *RabbitMessageQueue) ConnClose() error {
	if rq.MqConnection == nil {
		return nil
	}
	return rq.MqConnection.Close()
}

// ChannelClose : ChannelClose
func (rq *RabbitMessageQueue) ChannelClose() error {
	if rq.MqChannel == nil {
		return nil
	}
	return rq.MqChannel.Close()
}

func (rq *RabbitMessageQueue) InitSetPreFixed() error {
	var prefix = ""
	if rq.UsePreFix {
		prefix = rq.PreFix + rq.Sep
	} else {
		return errors.New("not usePreFix")
	}
	var (
		preQueue    = prefix + rq.QueueName
		preExchange = prefix + rq.Exchange
		preRoute    = prefix + rq.RoutingKey
	)
	rq.QueueName = preQueue
	rq.Exchange = preExchange
	rq.RoutingKey = preRoute
	return nil
}

// SimpleQueueDeclare : 1:1 로 메시지 전송(단일 노드) P -> Q -> C1
func (rq *RabbitMessageQueue) SimpleQueueDeclare() (resQueue *amqp.Queue, err error) {

	queName := rq.QueueName
	if queName == "" {
		log.Printf("SimpleQueueDeclare queName empty %v", queName)
		return resQueue, errors.New("header empty")
	}

	if rq.MqConnection == nil || rq.MqChannel == nil {
		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return resQueue, conErr
		}
	}
	if rq.MqChannel == nil {
		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return resQueue, conErr
		}
	}

	if rq.MqConnection != nil {
		mqChannel, errChannel := rmqConnection.Channel()
		if errChannel != nil {
			log.Printf("back_mq channel nil err %v", errChannel)
			return resQueue, errChannel
		}
		rq.MqChannel = mqChannel
	}

	var args amqp.Table = nil
	var queueDurable = false
	var autoDelete = false
	if rq.QueueType == "quorum" {
		args = amqp.Table{
			"x-queue-type": "quorum",
		}
		queueDurable = true
		autoDelete = false
	}
	q, decErr := rq.MqChannel.QueueDeclare(
		queName,      // name
		queueDurable, // durable
		autoDelete,   // delete when unused
		false,        // exclusive
		false,        // no-wait
		args,         // arguments
	)
	if decErr != nil {
		log.Printf("rq.MqChannel %v", rq.MqChannel)
		log.Printf("queName %v", queName)
		log.Printf("rq decErr %v", decErr)
		return resQueue, decErr
	}
	exchange := ""
	rq.Queue = &q
	rq.Exchange = exchange
	rq.QueueName = queName
	rq.RoutingKey = queName

	return rq.Queue, nil
}

// SimpleReceiveQueueDeclare : 1:1 로 메시지 수신(단일 노드) P -> Q -> C1
//func (rq *RabbitMessageQueue) SimpleReceiveQueueDeclare(queName *string) (resRq *RabbitMessageQueue, err error) {
//
//	if rq.MqConnection == nil || rq.MqChannel == nil {
//		conErr := rq.Connect()
//		if conErr != nil {
//			log.Printf("rq conErr %v", conErr)
//			return resRq, conErr
//		}
//	}
//	if rq.MqChannel == nil {
//		conErr := rq.Connect()
//		if conErr != nil {
//			log.Printf("rq conErr %v", conErr)
//			return resRq, conErr
//		}
//	}
//
//	var args amqp.Table = nil
//	var queueDurable = false
//	var autoDelete = false
//	if rq.QueueType == "quorum" {
//		args = amqp.Table{
//			"x-queue-type": "quorum",
//		}
//		queueDurable = true
//		autoDelete = false
//	}
//
//	rQueue, qErr := rq.MqChannel.QueueDeclare(
//		*queName,     // name
//		queueDurable, // durable
//		autoDelete,   // delete when unused
//		false,        // exclusive
//		false,        // no-wait
//		args,         // arguments
//	)
//	if qErr != nil {
//		return resRq, qErr
//	}
//	rq.Queue = &rQueue
//
//	return rq, nil
//	//if decErr != nil {
//	//	log.Printf("rq QueueDeclare %v", decErr)
//	//	return decErr
//	//}
//	//return nil
//}

// WorkQueuesQueueDeclare : QueueDeclare 경쟁 방식
func (rq *RabbitMessageQueue) WorkQueuesQueueDeclare() (resQueue *amqp.Queue, err error) {

	queueName := rq.QueueName
	exchange := rq.Exchange
	routingKey := rq.RoutingKey

	if rq.MqConnection == nil || rq.MqChannel == nil {
		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return resQueue, conErr
		}
	}
	if rq.MqChannel == nil {
		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return resQueue, conErr
		}
	}

	var args amqp.Table = nil
	var queueDurable = false
	var autoDelete = false
	if rq.QueueType == "quorum" {
		args = amqp.Table{
			"x-queue-type": "quorum",
		}
		queueDurable = true
		autoDelete = false
	}
	q, decErr := rq.MqChannel.QueueDeclare(
		queueName,    // name
		queueDurable, // durable
		autoDelete,   // delete when unused
		false,        // exclusive
		false,        // no-wait
		args,         // arguments
	)
	if decErr != nil {
		log.Printf("rq decErr %v", decErr)
		return resQueue, decErr
	}
	rq.Queue = &q
	rq.Exchange = exchange
	rq.QueueName = queueName
	rq.RoutingKey = queueName
	if routingKey != "" {
		rq.RoutingKey = routingKey
	}

	return rq.Queue, nil
}

// FanOutQueueDeclare : QueueDeclare publish / subscribe
func (rq *RabbitMessageQueue) FanOutQueueDeclare() (resQueue *amqp.Queue, err error) {

	queueName := rq.QueueName
	exchange := rq.Exchange
	routingKey := rq.RoutingKey

	if exchange == "" {
		log.Printf("exchange nil")
		return resQueue, errors.New("exchange nil")
	}

	if rq.MqConnection == nil || rq.MqChannel == nil {
		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return resQueue, conErr
		}
	}
	if rq.MqChannel == nil {
		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return resQueue, conErr
		}
	}

	var exchangeType string = "fanout"
	var args amqp.Table = nil
	var queueDurable = true
	var autoDelete = false
	if rq.QueueType == "quorum" {
		args = amqp.Table{
			"x-queue-type": "quorum",
		}
		queueDurable = true
		autoDelete = false
	}

	excErr := rq.MqChannel.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		queueDurable, // durable
		autoDelete,   // auto-deleted
		false,        // internal
		false,        // no-wait
		args,         // arguments
	)
	if excErr != nil {
		log.Printf("rq ExchangeDeclare %v", excErr)
		return resQueue, excErr
	}

	//rq.Queue = &q
	rq.Exchange = exchange
	rq.QueueName = queueName
	rq.RoutingKey = queueName
	if routingKey != "" {
		rq.RoutingKey = routingKey
	}

	return rq.Queue, nil
}
