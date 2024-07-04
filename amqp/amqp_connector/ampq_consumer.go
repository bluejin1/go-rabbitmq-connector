package amqp_connector

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"mq_connector/v3/amqp/amqp_config"
	"time"
)

func (rq *RabbitMessageQueue) StartCommonRabbitQueuesConsumer() (messages <-chan amqp.Delivery, err error) {
	err = rq.SetCommonRabbitQueuesConsumer()
	if err != nil {
		log.Printf("SetCommonRabbitQueuesConsumer error %v", err)
		return messages, err
	}
	return rq.RunCommonRabbitQueuesConsumer()
}

// SetCommonRabbitQueuesConsumer : consumer QueueDeclare & bind
func (rq *RabbitMessageQueue) SetCommonRabbitQueuesConsumer() (err error) {
	if rq.QueueDeclareType == amqp_config.QueueDeclareTypeDirect {
		return rq.SetQueueBindRabbitSimpleQueuesConsumer()
	} else if rq.QueueDeclareType == amqp_config.QueueDeclareTypeWorkQueue {
		return rq.SetQueueBindRabbitWorkQueuesConsumer()
	} else if rq.QueueDeclareType == amqp_config.QueueDeclareTypePubSub {
		return rq.SetQueueBindRabbitMessageQueueSubscribe()
	} else {
		return rq.SetQueueBindRabbitWorkQueuesConsumer()
	}
}

// RunCommonRabbitQueuesConsumer : run consumer queue message
func (rq *RabbitMessageQueue) RunCommonRabbitQueuesConsumer() (messages <-chan amqp.Delivery, err error) {
	if rq.QueueDeclareType == amqp_config.QueueDeclareTypeDirect {
		return rq.StartRabbitSimpleQueuesConsumer()
	} else if rq.QueueDeclareType == amqp_config.QueueDeclareTypeWorkQueue {
		return rq.StartRabbitWorkQueuesConsumer()
	} else if rq.QueueDeclareType == amqp_config.QueueDeclareTypePubSub {
		return rq.StartRabbitMessageQueueSubscribe()
	} else {
		return rq.StartRabbitWorkQueuesConsumer()
	}
}

// SetQueueBindRabbitSimpleQueuesConsumer : Simple Queues - consumer
func (rq *RabbitMessageQueue) SetQueueBindRabbitSimpleQueuesConsumer() (err error) {
	log.Printf(">>SetQueueBindRabbitSimpleQueuesConsumer<<")

	if rq.QueueName == "" {
		log.Printf("queName empty %v", rq.QueueName)
		return errors.New("StartRabbitWorkQueuesConsumer queName empty")
	}

	rq.concurrency = 1

	if rq.MqConnection == nil || rq.MqChannel == nil {
		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return conErr
		}
	}
	if rq.MqChannel == nil {
		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return conErr
		}
	}

	_, rqErr := rq.SimpleQueueDeclare()
	if rqErr != nil {
		log.Printf("SimpleReceiveQueueDeclare conErr %v", rqErr)
		return errors.New("MqConnection nil err OR MqChannel nil Err")
	}

	// bind the queue to the routing key
	//err = rq.MqChannel.QueueBind(*queName, *routingKey, *exchange, false, nil)
	//if err != nil {
	//	log.Printf("QueueBind err %v", err)
	//	return messages, err
	//}

	// prefetch 4x as many messages as we can handle at once
	prefetchCount := rq.concurrency * 4
	err = rq.MqChannel.Qos(prefetchCount, 0, false)
	if err != nil {
		log.Printf("Qos err %v", err)
		return err
	}
	return nil
}

// SetQueueBindRabbitWorkQueuesConsumer : Work Queues - consumer
func (rq *RabbitMessageQueue) SetQueueBindRabbitWorkQueuesConsumer() (err error) {
	log.Printf(">>SetQueueBindRabbitWorkQueuesConsumer<<")

	if rq.QueueName == "" {
		log.Printf("queName empty %v", rq.QueueName)
		return errors.New("StartRabbitWorkQueuesConsumer queName empty")
	}

	rq.concurrency = 1

	if rq.MqConnection == nil || rq.MqChannel == nil {
		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return conErr
		}
	}
	if rq.MqChannel == nil {
		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return conErr
		}
	}

	_, rqErr := rq.WorkQueuesQueueDeclare()
	if rqErr != nil {
		log.Printf("SimpleReceiveQueueDeclare conErr %v", rqErr)
		return errors.New("MqConnection nil err OR MqChannel nil Err")
	}

	// bind the queue to the routing key
	//err = rq.MqChannel.QueueBind(*queName, *routingKey, *exchange, false, nil)
	//if err != nil {
	//	log.Printf("QueueBind err %v", err)
	//	return messages, err
	//}

	// prefetch 4x as many messages as we can handle at once
	prefetchCount := rq.concurrency * 4
	err = rq.MqChannel.Qos(prefetchCount, 0, false)
	if err != nil {
		log.Printf("Qos err %v", err)
		return err
	}

	return nil
}

// SetQueueBindRabbitMessageQueueSubscribe : Publish/Subscribe - Subscribe
func (rq *RabbitMessageQueue) SetQueueBindRabbitMessageQueueSubscribe() (err error) {

	if rq.MqConnection == nil || rq.MqChannel == nil {
		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return conErr
		}
	}
	if rq.MqConnection == nil || rq.MqChannel == nil {
		log.Printf("MqConnection nil err OR MqChannel nil Err")
		return errors.New("MqConnection nil err OR MqChannel nil Err")
	}

	exchangeDeclareErr := rq.MqChannel.ExchangeDeclare(
		rq.Exchange, // name
		"fanout",    // type
		false,       // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if exchangeDeclareErr != nil {
		fmt.Println("ExchangeDeclare Error:", exchangeDeclareErr)
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

	queue, queueDeclareErr := rq.MqChannel.QueueDeclare(
		rq.QueueName, // name
		queueDurable, // durable
		autoDelete,   // delete when unused
		true,         // exclusive
		false,        // no-wait
		args,         // arguments
	)
	if queueDeclareErr != nil {
		fmt.Println("QueueDeclare Error:", queueDeclareErr)
		return errors.New("QueueDeclare Error")
	}

	rq.Queue = &queue
	//rq.Exchange = exchange
	//rq.QueueName = queName
	//rq.RoutingKey = queName

	queueBindErr := rq.MqChannel.QueueBind(
		queue.Name,  // queue name
		"",          // routing key
		rq.Exchange, // exchange
		true,
		nil,
	)
	if queueBindErr != nil {
		fmt.Println("QueueBind Error:", queueBindErr)
		return queueBindErr
	}
	return nil
}

// StartRabbitSimpleQueuesConsumer : Simple Queues - consumer
func (rq *RabbitMessageQueue) StartRabbitSimpleQueuesConsumer() (messages <-chan amqp.Delivery, err error) {
	log.Printf(">>StartRabbitSimpleQueuesConsumer<<")

	if rq.QueueName == "" {
		log.Printf("queName empty %v", rq.QueueName)
		return messages, errors.New("StartRabbitWorkQueuesConsumer queName empty")
	}

	if rq.MqConnection == nil || rq.MqChannel == nil {
		err = errors.New("MqConnection | MqChannel error")
		log.Printf("MqChannel error %v", err)
		return messages, err
	}

	msgs, conErr := rq.MqChannel.Consume(
		rq.Queue.Name, // queue
		"",            // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)

	if conErr != nil {
		log.Printf("Consume err %v", err)
		return messages, err
	}

	messages = msgs

	return messages, nil
}

// StartRabbitWorkQueuesConsumer : Work Queues - consumer
func (rq *RabbitMessageQueue) StartRabbitWorkQueuesConsumer() (messages <-chan amqp.Delivery, err error) {
	log.Printf(">>StartRabbitWorkQueuesConsumer<<")

	if rq.QueueName == "" {
		log.Printf("queName empty %v", rq.QueueName)
		return messages, errors.New("StartRabbitWorkQueuesConsumer queName empty")
	}
	if rq.MqConnection == nil || rq.MqChannel == nil {
		err = errors.New("MqConnection | MqChannel error")
		log.Printf("MqChannel error %v", err)
		return messages, err
	}

	msgs, conErr := rq.MqChannel.Consume(
		rq.Queue.Name, // queue
		"",            // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)

	if conErr != nil {
		log.Printf("Consume err %v", err)
		return messages, err
	}

	messages = msgs

	return messages, nil
}

// StartRabbitMessageQueueSubscribe : Publish/Subscribe - Subscribe
func (rq *RabbitMessageQueue) StartRabbitMessageQueueSubscribe() (messages <-chan amqp.Delivery, err error) {

	if rq.QueueName == "" {
		log.Printf("queName empty %v", rq.QueueName)
		return messages, errors.New("StartRabbitWorkQueuesConsumer queName empty")
	}
	if rq.MqConnection == nil || rq.MqChannel == nil {
		err = errors.New("MqConnection | MqChannel error")
		log.Printf("MqChannel error %v", err)
		return messages, err
	}
	if rq.MqConnection == nil || rq.MqChannel == nil {
		log.Printf("MqConnection nil err OR MqChannel nil Err")
		return messages, errors.New("MqConnection nil err OR MqChannel nil Err")
	}

	//exchangeDeclareErr := rq.MqChannel.ExchangeDeclare(
	//	rq.Exchange, // name
	//	"fanout",    // type
	//	false,       // durable
	//	false,       // auto-deleted
	//	false,       // internal
	//	false,       // no-wait
	//	nil,         // arguments
	//)
	//if exchangeDeclareErr != nil {
	//	fmt.Println("ExchangeDeclare Error:", exchangeDeclareErr)
	//}
	//
	//var args amqp.Table = nil
	//var queueDurable = false
	//var autoDelete = false
	//if rq.QueueType == "quorum" {
	//	args = amqp.Table{
	//		"x-queue-type": "quorum",
	//	}
	//	queueDurable = true
	//	autoDelete = false
	//}
	//
	//queue, queueDeclareErr := rq.MqChannel.QueueDeclare(
	//	rq.QueueName, // name
	//	queueDurable, // durable
	//	autoDelete,   // delete when unused
	//	true,         // exclusive
	//	false,        // no-wait
	//	args,         // arguments
	//)
	//if queueDeclareErr != nil {
	//	fmt.Println("QueueDeclare Error:", queueDeclareErr)
	//}
	//
	//queueBindErr := rq.MqChannel.QueueBind(
	//	queue.Name,  // queue name
	//	"",          // routing key
	//	rq.Exchange, // exchange
	//	true,
	//	nil,
	//)
	//if queueBindErr != nil {
	//	fmt.Println("QueueBind Error:", queueBindErr)
	//	return nil, queueBindErr
	//}

	message, consumeErr := rq.MqChannel.Consume(
		rq.Queue.Name, // queue
		"",            // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if consumeErr != nil {
		fmt.Println("Consume Error:", consumeErr)
		return nil, consumeErr
	}

	return message, nil
}

func (rq *RabbitMessageQueue) ConsumerHandle(deliveries <-chan amqp.Delivery) (err error) {

	//queue := rq.QueueName
	//routingKey := rq.RoutingKey
	//threads := amqp_helper.GetMaxParallelism()

	var forever chan struct{}
	go func() {
		if rq.ShowLog {
			log.Printf("Enter go with thread with deliveries %v", deliveries)
		}
		for msg := range deliveries {
			if rq.ShowLog {
				log.Printf("Enter deliver")
			}
			go func(sMsg amqp.Delivery) {
				rtErr := rq.ConsumerHandlerFun(&sMsg)
				if rtErr != nil {
					log.Printf("ConsumerHandlerFun Err %v", rtErr)
					//msg.Nack(false, true)
					//rq.currentStatus.Store(false)
				}
				if rq.ShowLog {
					log.Printf("ConsumerHandlerFun message receive success")
				}

				currentTime := time.Now().UTC().Unix()
				//if currentTime-rq.lastRecoverTime > amqp_config.RECOVER_INTERVAL_TIME && !rq.currentStatus.Load().(bool) {
				//log.Printf("Try to Recover Unack Messages!")
				rq.currentStatus.Store(true)
				rq.lastRecoverTime = currentTime
				//rq.MqChannel.Recover(true)
				//}
			}(msg)

			//if ret == true {
			//	msg.Ack(false)
			//	currentTime := time.Now().Unix()
			//	if currentTime-rq.lastRecoverTime > amqp_config.RECOVER_INTERVAL_TIME && !rq.currentStatus.Load().(bool) {
			//		log.Printf("Try to Recover Unack Messages!")
			//		rq.currentStatus.Store(true)
			//		rq.lastRecoverTime = currentTime
			//		//rq.MqChannel.Recover(true)
			//	}
			//} else {
			//	// this really a litter dangerous. if the worker is panic very quickly,
			//	// it will ddos our sentry server......plz, add [retry-ttl] in header.
			//	msg.Nack(false, true)
			//	//rq.currentStatus.Store(false)
			//}

			//dotCount := bytes.Count(msg.Body, []byte("."))
			//t := time.Duration(dotCount)
			//time.Sleep(t * time.Second)
			if rq.ShowLog {
				log.Printf("msg Receive Done")
			}

			msg.Ack(false)
		}
	}()
	if rq.ShowLog {
		log.Printf("ConsumerHandle [*] Waiting for messages")
	}

	<-forever

	return nil
}
