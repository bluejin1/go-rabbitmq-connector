package amqp

import (
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"mq_connector/v3/amqp/amqp_config"
	"mq_connector/v3/amqp/amqp_connector"
	"mq_connector/v3/connector_types"
)

// SetConsumer : consumerMq 리턴을 위함
func SetConsumer(mqConsumer *connector_types.Consumer) (res *amqp_connector.RabbitMessageQueue, err error) {
	log.Printf(">>> SetConsumer <<<")

	if mqConsumer.QueName == "" {
		log.Printf("queue is nil")
		return res, errors.New("queue is nil")
	}

	rmqConfig := amqp_config.DefaultRabbitMQServerConfigFromEnvWsConnector()
	if rmqConfig == nil {
		log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", rmqConfig)
		return res, errors.New("DefaultRabbitMQServerConfigFromEnvWsConnector nil err ")
	}
	if mqConsumer.ShowLog {
		rmqConfig.ShowLog = mqConsumer.ShowLog
	}
	if rmqConfig.ShowLog {
		log.Printf("rmqConfig:: %v", rmqConfig)
	}
	rmqConfig.QueueDeclareType = amqp_config.QueueDeclareTypeWorkQueue
	if mqConsumer.QueueDeclareType != "" {
		rmqConfig.QueueDeclareType = mqConsumer.QueueDeclareType
	}

	consumerMq, nErr := amqp_connector.SetNewConsumer(mqConsumer.QueName, mqConsumer.Exchange, mqConsumer.RoutingKey, rmqConfig)
	if nErr != nil {
		log.Printf("SetNewConsumer nil %v", consumerMq)
		return res, errors.New("SetNewConsumer nil err")
	}

	return consumerMq, nil
}

// StartConsumerQueue : StartConsumerQueue message 리턴을 위함
func StartConsumerQueue(consumerMq *amqp_connector.RabbitMessageQueue, megDeliveryHandler *func(*amqp.Delivery) error) (err error) {
	log.Printf(">>> SetConsumer <<<")

	if consumerMq == nil {
		log.Printf("SetNewConsumer consumerMq nil %v", consumerMq)
		return errors.New("consumerMq nil")
	}

	if consumerMq.Queue == nil {
		log.Printf("queue is nil")
		return errors.New("queue is nil")
	}

	if consumerMq.Queue.Name == "" {
		log.Printf("queue is nil")
		return errors.New("queue is nil")
	}

	if megDeliveryHandler == nil {
		log.Printf("megDeliveryHandler is nil")
		return errors.New("megDeliveryHandler is nil")
	}

	hErr := amqp_connector.RunConsumer(consumerMq, megDeliveryHandler)
	if hErr != nil {
		log.Printf("ConsumerHandle error %v", hErr)
	}
	return hErr

}
