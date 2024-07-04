package amqp_config

import (
	"errors"
	"mq_connector/v3/amqp/amqp_helper"
	"mq_connector/v3/connector_env"
	"strconv"
	"sync"
)

var (
	RmqConfig     *RmqServer
	rmqConfigOnce sync.Once
)

type AddressInfo struct {
	Host string // localhost,
	Port string
}

type RmqPreFix struct {
	UsePreFix string
	Sep       string
	PreFix    string
}

type RmqServer struct {
	MqType           string
	Timeout          int
	Pool             int
	User             string
	Password         string
	MqClusters       string
	Address          AddressInfo
	ExchangeType     string
	QueueType        string
	QueueDeclareType string
	Exchange         string
	RoutingKey       string
	QueueName        string
	ShowLog          bool
	RmqPreFix        RmqPreFix
	// AWS TLS
	MqUseTLS         bool
	MqTLSRootCAsFile string
	MqTLSCertFile    string
	MqTLSKeyFile     string

	IsPublisher bool
	IsConsumer  bool
}

func DefaultRabbitMQServerConfigFromEnvWsConnector() *RmqServer {
	if RmqConfig != nil {
		return RmqConfig
	}
	rmqConfigOnce.Do(func() {
		timeout, _ := strconv.Atoi(amqp_helper.GetEnv("MQ_TIMEOUT", connector_env.MQ_TIMEOUT))
		pool, _ := strconv.Atoi(amqp_helper.GetEnv("MQ_POOL_SIZE", connector_env.MQ_POOL_SIZE))
		mqUseTLS := amqp_helper.GetEnvBool("MQ_USE_TLS", false)
		getMqUseTLS := amqp_helper.GetEnv("MQ_USE_TLS", connector_env.MQ_USE_TLS)

		showLog := false
		showLogVal := amqp_helper.GetEnv("MQ_POOL_SHOW_LOG", connector_env.MQ_POOL_SHOW_LOG)
		if showLogVal == "true" {
			showLog = true
		}

		if getMqUseTLS == "true" {
			mqUseTLS = true
		}

		RmqConfig = &RmqServer{
			MqType:     amqp_helper.GetEnv("MQ_TYPE", connector_env.MQ_TYPE),
			Timeout:    timeout,
			Pool:       pool,
			User:       amqp_helper.GetEnv("MQ_USER", connector_env.MQ_USER),
			Password:   amqp_helper.GetEnv("MQ_PW", connector_env.MQ_PW),
			MqClusters: amqp_helper.GetEnv("MQ_CLUSTERS", connector_env.MQ_CLUSTERS),
			QueueType:  amqp_helper.GetEnv("MQ_QUEUE_TYPE", connector_env.MQ_QUEUE_TYPE),
			RmqPreFix: RmqPreFix{
				UsePreFix: amqp_helper.GetEnv("MQ_PREFIX_USE", connector_env.MQ_PREFIX_USE),
				PreFix:    amqp_helper.GetEnv("MQ_PREFIX_SEP", connector_env.MQ_PREFIX_SEP),
				Sep:       amqp_helper.GetEnv("MQ_PREFIX_PREFIX", connector_env.MQ_PREFIX_PREFIX),
			},
			Address: AddressInfo{
				Host: amqp_helper.GetEnv("MQ_HOST", connector_env.MQ_HOST),
				Port: amqp_helper.GetEnv("MQ_PORT", connector_env.MQ_PORT),
			},
			ShowLog: showLog,
			// AWS TLS
			MqUseTLS:         mqUseTLS,
			MqTLSRootCAsFile: amqp_helper.GetEnv("MQ_TLS_ROOTCAS_FILE", connector_env.MQ_TLS_ROOTCAS_FILE),
			MqTLSCertFile:    amqp_helper.GetEnv("MQ_TLS_CERT_FILE", connector_env.MQ_TLS_CERT_FILE),
			MqTLSKeyFile:     amqp_helper.GetEnv("MQ_TLS_KEY_FILE", connector_env.MQ_TLS_KEY_FILE),
		}
	})
	return RmqConfig
}

func SetRabbitMQServerConfigFromEnvWsConnector(newRmqConfig *RmqServer) (res *RmqServer, err error) {
	if newRmqConfig == nil {
		return nil, errors.New("newRmqConfig nil")
	}
	RmqConfig = newRmqConfig
	return RmqConfig, nil
}
