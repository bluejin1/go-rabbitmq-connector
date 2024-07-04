package amqp_config

import "time"

const (
	ReconnectDelay            = 10 * time.Second
	ResendDelay               = 5 * time.Second
	ResendTime                = 3
	MaxWaitNum                = 5
	DefSep                    = "_"
	DefTtl                int = 60 * 1000
	RECOVER_INTERVAL_TIME     = 6 * 60
)

const (
	QueueDeclareTypeDirect    string = "Direct"
	QueueDeclareTypeWorkQueue string = "WorkQueue"
	QueueDeclareTypePubSub    string = "PubSub"
)

// string // fanout, topic, direct
const (
	ExchangeTypeDefault string = ""
	ExchangeTypeFanout  string = "fanout"
	ExchangeTypeTopic   string = "topic"
	ExchangeTypeDirect  string = "direct"
)

// test
const (
	RabbitMQTaskRootTopicId string = ""
	RabbitMQDcConfigTopicId string = ""
)
