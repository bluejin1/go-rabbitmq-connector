package amqp

import (
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"mq_connector/v3/amqp/amqp_config"
	"mq_connector/v3/amqp/amqp_connector"
	"mq_connector/v3/connector_types"
)

// RunConsumer : QueueDeclareType string //Direct(simple), WorkQueue, PubSub
func RunConsumer(mqConsumer *connector_types.Consumer, megDeliveryHandler *func(*amqp.Delivery) error) (res *amqp_connector.RabbitMessageQueue, err error) {
	log.Printf(">>> RunConsumer <<<")

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
	res = consumerMq

	if consumerMq == nil {
		log.Printf("SetNewConsumer consumerMq nil %v", consumerMq)
		return res, errors.New("consumerMq nil")
	}

	if consumerMq.Queue.Name == "" {
		log.Printf("queue is nil")
		return res, errors.New("queue is nil")
	}

	if megDeliveryHandler == nil {
		log.Printf("megDeliveryHandler is nil")
		return res, errors.New("megDeliveryHandler is nil")
	}

	hErr := amqp_connector.RunConsumer(consumerMq, megDeliveryHandler)
	if hErr != nil {
		log.Printf("ConsumerHandle error %v", hErr)
	}
	return res, hErr

}

// RunConsumerWithConfig : QueueDeclareType string //Direct(simple), WorkQueue, PubSub
func RunConsumerWithConfig(mqConsumer *connector_types.Consumer, megDeliveryHandler *func(*amqp.Delivery) error, newRmqConfig *amqp_config.RmqServer) (res *amqp_connector.RabbitMessageQueue, err error) {
	log.Printf(">>> RunConsumerWithConfig <<<")

	if mqConsumer.QueName == "" {
		log.Printf("queue is nil")
		return res, errors.New("queue is nil")
	}
	if newRmqConfig == nil {
		log.Printf("newRmqConfig is nil")
		return res, errors.New("newRmqConfig is nil")
	}

	rmqConfig, cErr := amqp_config.SetRabbitMQServerConfigFromEnvWsConnector(newRmqConfig)
	if cErr != nil {
		log.Printf("SetRabbitMQServerConfigFromEnvWsConnector err %v", cErr)
		return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
	}
	if rmqConfig == nil {
		log.Printf("SetRabbitMQServerConfigFromEnvWsConnector nil %v", rmqConfig)
		return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector nil err")
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

	//consumerMq, nErr := amqp_connector.NewConsumer(mqConsumer.QueName, mqConsumer.Exchange, mqConsumer.RoutingKey, megDeliveryHandler, rmqConfig)
	//if nErr != nil {
	//	log.Printf("NewConsumer nil %v", nErr)
	//	return res, errors.New("NewConsumer nil err")
	//}
	//
	//return consumerMq, nil

	consumerMq, nErr := amqp_connector.SetNewConsumer(mqConsumer.QueName, mqConsumer.Exchange, mqConsumer.RoutingKey, rmqConfig)
	if nErr != nil {
		log.Printf("SetNewConsumer nil %v", consumerMq)
		return res, errors.New("SetNewConsumer nil err")
	}
	res = consumerMq

	if consumerMq == nil {
		log.Printf("SetNewConsumer consumerMq nil %v", consumerMq)
		return res, errors.New("consumerMq nil")
	}

	if consumerMq.Queue.Name == "" {
		log.Printf("queue is nil")
		return res, errors.New("queue is nil")
	}

	if megDeliveryHandler == nil {
		log.Printf("megDeliveryHandler is nil")
		return res, errors.New("megDeliveryHandler is nil")
	}

	hErr := amqp_connector.RunConsumer(consumerMq, megDeliveryHandler)
	if hErr != nil {
		log.Printf("ConsumerHandle error %v", hErr)
	}
	return res, hErr
}

// SetPublisher : ExchangeType     string // fanout, topic, direct, etc..., QueueDeclareType string //Direct(simple), WorkQueue, PubSub
func SetPublisher(mqPublisher *connector_types.Publisher) (res *amqp_connector.RabbitMessageQueue, err error) {
	log.Printf(">>> SetPublisher <<<")

	if mqPublisher.QueName == "" {
		log.Printf("queue is nil")
		return res, errors.New("queue is nil")
	}

	rmqConfig := amqp_config.DefaultRabbitMQServerConfigFromEnvWsConnector()
	if rmqConfig == nil {
		log.Printf("DefaultRabbitMQServerConfigFromEnvWsConnector nil %v", rmqConfig)
		return res, errors.New("DefaultRabbitMQServerConfigFromEnvWsConnector nil err")
	}
	if mqPublisher.ShowLog {
		rmqConfig.ShowLog = mqPublisher.ShowLog
	}
	if rmqConfig.ShowLog {
		log.Printf("rmqConfig:: %v", rmqConfig)
	}

	rmqConfig.QueueDeclareType = amqp_config.QueueDeclareTypeWorkQueue
	if mqPublisher.QueueDeclareType != "" {
		rmqConfig.QueueDeclareType = mqPublisher.QueueDeclareType
	}

	log.Printf("rmqConfig %v", rmqConfig)
	log.Printf("mqPublisher %v", mqPublisher)

	publisherMq, nErr := amqp_connector.NewPublisher(mqPublisher.QueName, mqPublisher.Exchange, mqPublisher.RoutingKey, mqPublisher.ExchangeType, rmqConfig)
	if nErr != nil {
		log.Printf("NewPublisher nil %v", nErr)
		return res, errors.New("NewPublisher nil err")
	}

	return publisherMq, nil
}

// SetPublisherWithConfig : ExchangeType     string // fanout, topic, direct, etc..., QueueDeclareType string //Direct(simple), WorkQueue, PubSub
func SetPublisherWithConfig(mqPublisher *connector_types.Publisher, newRmqConfig *amqp_config.RmqServer) (res *amqp_connector.RabbitMessageQueue, err error) {
	log.Printf(">>> SetPublisherWithConfig <<<")

	if mqPublisher.QueName == "" {
		log.Printf("queue is nil")
		return res, errors.New("queue is nil")
	}

	rmqConfig, cErr := amqp_config.SetRabbitMQServerConfigFromEnvWsConnector(newRmqConfig)
	if cErr != nil {
		log.Printf("SetRabbitMQServerConfigFromEnvWsConnector err %v", cErr)
		return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector err")
	}
	if rmqConfig == nil {
		log.Printf("SetRabbitMQServerConfigFromEnvWsConnector nil %v", rmqConfig)
		return res, errors.New("SetRabbitMQServerConfigFromEnvWsConnector nil err")
	}
	if mqPublisher.ShowLog {
		rmqConfig.ShowLog = mqPublisher.ShowLog
	}
	if rmqConfig.ShowLog {
		log.Printf("rmqConfig:: %v", rmqConfig)
	}

	rmqConfig.QueueDeclareType = amqp_config.QueueDeclareTypeWorkQueue
	if mqPublisher.QueueDeclareType != "" {
		rmqConfig.QueueDeclareType = mqPublisher.QueueDeclareType
	}

	publisherMq, nErr := amqp_connector.NewPublisher(mqPublisher.QueName, mqPublisher.Exchange, mqPublisher.RoutingKey, mqPublisher.ExchangeType, rmqConfig)
	if nErr != nil {
		log.Printf("NewPublisher nil %v", nErr)
		return res, errors.New("NewPublisher nil err")
	}

	return publisherMq, nil
}

// RunPublishMessage :
func RunPublishMessage(amqpConnector *amqp_connector.RabbitMessageQueue, pushMsg *connector_types.PublishMessage) error {
	log.Printf(">>> RunPublishMessage <<<")

	if amqpConnector == nil {
		log.Printf("amqpConnector is nil")
		return errors.New("amqpConnector is nil")
	}

	var err error = nil
	if amqpConnector.QueueName == "" {
		log.Printf("queue is nil")
		return errors.New("queue is nil")
	}

	if amqpConnector.QueueDeclareType == amqp_config.QueueDeclareTypeDirect {
		err = amqpConnector.SimplePublish(pushMsg.MsgHeader, pushMsg.MsgBody, pushMsg.MessageId, pushMsg.MsgUserId)
	} else if amqpConnector.QueueDeclareType == amqp_config.QueueDeclareTypeWorkQueue {
		err = amqpConnector.WorkQueuesPublish(pushMsg.MsgHeader, pushMsg.MsgBody, pushMsg.MessageId, pushMsg.MsgUserId)
	} else if amqpConnector.QueueDeclareType == amqp_config.QueueDeclareTypePubSub {
		err = amqpConnector.FanOutPublish(pushMsg.MsgHeader, pushMsg.MsgBody, pushMsg.MessageId, pushMsg.MsgUserId)
	} else {
		err = amqpConnector.WorkQueuesPublish(pushMsg.MsgHeader, pushMsg.MsgBody, pushMsg.MessageId, pushMsg.MsgUserId)
	}

	return err
}
