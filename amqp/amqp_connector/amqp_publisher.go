package amqp_connector

import (
	"context"
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

// SimplePublish : 1:1 로 메시지 전송(단일 노드) P -> Q -> C1
func (rq *RabbitMessageQueue) SimplePublish(header *map[string]interface{}, body *[]byte, MessageId string, userId string) error {
	log.Printf(">>> SimplePublish <<<")
	if ok, err := rq.waitConn(); !ok {
		log.Printf("waitConn not ok")
		return err
	}

	//queName := rq.QueueName
	//exchange := rq.Exchange

	if header == nil {
		log.Printf("SimplePublish header empty %v", header)
		return errors.New("header empty")
	}

	if body == nil {
		log.Printf("SimplePublish body empty %v", body)
		return errors.New("header empty")
	}

	if rq.MqConnection == nil || rq.MqChannel == nil {
		log.Printf("rq.MqConnection nil %v", rq.MqConnection)
		log.Printf("rq.MqChannel nil %v", rq.MqChannel)
		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return conErr
		}
	}
	if rq.MqChannel == nil {
		log.Printf("rq.MqChannel nil %v", rq.MqChannel)

		conErr := rq.Connect()
		if conErr != nil {
			log.Printf("rq conErr %v", conErr)
			return conErr
		}
	}

	if rq.Queue == nil {
		if rq.MqConnection == nil || rq.MqChannel == nil {
			conErr := rq.Connect()
			if conErr != nil {
				log.Printf("rq conErr %v", conErr)
				return conErr
			}
		}
		_, rqErr := rq.SimpleQueueDeclare()
		if rqErr != nil {
			log.Printf("SimpleQueueDeclare conErr %v", rqErr)
			return rqErr
		}
	}

	//ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rq.MqConfig.Timeout)*time.Second)
	//defer cancel()
	//
	////MessageId := ""
	//pubTime := time.Now().UTC()
	//userId = ""
	//
	//return rq.MqChannel.PublishWithContext(ctx,
	//	exchange,            // exchange
	//	queName, // routing key
	//	false,         // mandatory
	//	false,         // immediate
	//	amqp.Publishing{
	//		ContentType: "application/json",
	//		Headers:     *header,
	//		MessageId:   MessageId,
	//		Timestamp:   pubTime,
	//		UserId:      userId,
	//		Body:        *body,
	//	})
	return rq.UnsafePush(MessageId, header, body)
}

// WorkQueuesPublish : QueuesPublish 경쟁 방식
func (rq *RabbitMessageQueue) WorkQueuesPublish(header *map[string]interface{}, body *[]byte, MessageId string, userId string) error {
	log.Printf(">> WorkQueuesPublish <<")

	if ok, err := rq.waitConn(); !ok {
		log.Printf("waitConn not ok")
		return err
	}
	//queueName := rq.QueueName
	////routingKey := rq.RoutingKey
	//exchange := rq.Exchange

	if header == nil {
		log.Printf("SimplePublish header empty %v", header)
		return errors.New("header empty")
	}
	if body == nil {
		log.Printf("SimplePublish body empty %v", body)
		//return errors.New("header empty")
		body = &[]byte{}
	}

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

	if rq.Queue == nil {
		if rq.MqConnection == nil || rq.MqChannel == nil {
			conErr := rq.Connect()
			if conErr != nil {
				log.Printf("rq conErr %v", conErr)
				return conErr
			}
		}
		_, rqErr := rq.WorkQueuesQueueDeclare()
		if rqErr != nil {
			log.Printf("WorkQueuesQueueDeclare conErr %v", rqErr)
			return rqErr
		}
	}

	//ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rq.MqConfig.Timeout)*time.Second)
	//defer cancel()
	//
	////MessageId := ""
	//pubTime := time.Now().UTC()
	//userId = ""
	//
	//return rq.MqChannel.PublishWithContext(ctx,
	//	exchange,  // exchange
	//	queueName, // key
	//	false,     // mandatory
	//	false,     // immediate
	//	amqp.Publishing{
	//		ContentType: "application/json",
	//		Headers:     *header,
	//		MessageId:   MessageId,
	//		Timestamp:   pubTime,
	//		UserId:      userId,
	//		Body:        *body,
	//	})
	return rq.UnsafePush(MessageId, header, body)
}

// FanOutPublish : Publish/Subscribe - 다중
func (rq *RabbitMessageQueue) FanOutPublish(header *map[string]interface{}, body *[]byte, MessageId string, userId string) error {
	if rq.ShowLog {
		log.Printf(">> FanOutPublish <<")
	}
	if ok, err := rq.waitConn(); !ok {
		log.Printf("waitConn not ok")
		return err
	}

	//queueName := rq.QueueName
	////routingKey := rq.RoutingKey
	//exchange := rq.Exchange

	if header == nil {
		log.Printf("SimplePublish header empty %v", header)
		return errors.New("header empty")
	}
	if body == nil {
		log.Printf("SimplePublish body empty %v", body)
		//return errors.New("header empty")
		body = &[]byte{}
	}

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

	if rq.Queue == nil {
		if rq.MqConnection == nil || rq.MqChannel == nil {
			conErr := rq.Connect()
			if conErr != nil {
				log.Printf("rq conErr %v", conErr)
				return conErr
			}
		}
		_, rqErr := rq.FanOutQueueDeclare()
		if rqErr != nil {
			log.Printf("WorkQueuesQueueDeclare conErr %v", rqErr)
			return rqErr
		}
	}

	//ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rq.MqConfig.Timeout)*time.Second)
	//defer cancel()
	//
	////MessageId := ""
	//pubTime := time.Now().UTC()
	//userId = ""
	//
	//return rq.MqChannel.PublishWithContext(ctx,
	//	exchange,  // exchange
	//	queueName, // routing key
	//	false,     // mandatory
	//	false,     // immediate
	//	amqp.Publishing{
	//		ContentType: "application/json",
	//		Headers:     *header,
	//		MessageId:   MessageId,
	//		Timestamp:   pubTime,
	//		UserId:      userId,
	//		Body:        *body,
	//	})

	return rq.UnsafePush(MessageId, header, body)
}

func (rq *RabbitMessageQueue) UnsafePush(MessageId string, header *map[string]interface{}, body *[]byte) error {
	if rq.ShowLog {
		log.Printf(">>> UnsafePush <<< ")
	}
	if ok, err := rq.waitConn(); !ok {
		log.Printf("waitConn not ok")
		return err
	}
	queueName := rq.QueueName
	//routingKey := rq.RoutingKey
	exchange := rq.Exchange

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rq.MqConfig.Timeout)*time.Second)
	defer cancel()

	//MessageId := ""
	pubTime := time.Now().UTC()
	userId := ""

	pubErr := rq.MqChannel.PublishWithContext(ctx,
		exchange,  // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Headers:     *header,
			MessageId:   MessageId,
			Timestamp:   pubTime,
			UserId:      userId,
			Body:        *body,
		})

	if pubErr != nil {
		log.Printf("PublishWithContext Err %v", pubErr)
		return pubErr
	}
	if rq.ShowLog {
		log.Printf("PublishWithContext Success Queue:%v", queueName)
	}
	return nil
}
